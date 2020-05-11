package org.rzlabs.elastic

import com.fasterxml.jackson.core.Base64Variants
import org.apache.http.concurrent.Cancellable
import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{MyLogging, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLTimestamp
import org.apache.spark.sql.sources.elastic.{CloseableIterator, DummyResultIterator}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.rzlabs.elastic.client.{CancellableHolder, ConnectionManager, ElasticClient, ResultRow}
import org.rzlabs.elastic.metadata.ElasticRelationInfo
import org.rzlabs.elasticsearch.ElasticQuery

import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Success, Try}

case class ElasticPartition(index: Int, info: ElasticRelationInfo) extends Partition {

  def queryClient(httpMaxConnPerRoute: Int, httpMaxConnTotal: Int) = {
    ConnectionManager.init(httpMaxConnPerRoute, httpMaxConnTotal)
    new ElasticClient(info.options.host)
  }
}

class ElasticRDD(sqlContext: SQLContext,
                 info: ElasticRelationInfo,
                 val elasticQuery: ElasticQuery
                ) extends RDD[InternalRow](sqlContext.sparkContext, Nil) {

  val httpMaxConnPerRoute = info.options.poolMaxConnectionsPerRoute
  val httpMaxConnTotal = info.options.poolMaxConnections
  val schema: StructType = elasticQuery.schema(info)

  // TODO: add recording Elastic query logic.

  override def getPartitions: Array[Partition] = {
    Array(new ElasticPartition(0, info))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val partition = split.asInstanceOf[ElasticPartition]
    val qrySpec = elasticQuery.qrySpec

    var cancelCallback: TaskCancelHandler.TaskCancelHolder = null
    var resultIter: CloseableIterator[ResultRow] = null
    var client: ElasticClient = null
    val queryId = s"query-${System.nanoTime()}"
    var queryStartTime = System.currentTimeMillis()
    var queryStartDT = new DateTime().toString()
    try {
      cancelCallback = TaskCancelHandler.registerQueryId(queryId, context)
      client = partition.queryClient(httpMaxConnPerRoute, httpMaxConnTotal)
      client.setCancellableHolder(cancelCallback)
      queryStartTime = System.currentTimeMillis()
      queryStartDT = new DateTime().toString()
      resultIter = qrySpec.executeQuery(client)
    } catch {
      case _ if cancelCallback.wasCancelTriggered && client != null =>
        resultIter = new DummyResultIterator()
      case e: Throwable => throw e
    } finally {
      TaskCancelHandler.clearQueryId(queryId)
    }

    val elasticExecTime = System.currentTimeMillis() - queryStartTime
    var numRows: Int = 0

    context.addTaskCompletionListener { taskContext =>
      resultIter.closeIfNeeded()
    }

    val rIter = new InterruptibleIterator[ResultRow](context, resultIter)
    val nameToTF: Map[String, String] = elasticQuery.getValTFMap()

    rIter.map { r =>
      numRows += 1
      val row = new GenericInternalRow(schema.fields.map { field =>
        info.indexInfo.columns.get(field.name) match {
          case Some(ec) =>
            val value = r.event.get(field.name) match {
              case Some(v) => v
              case _ if info.options.nullFillNonexistentFieldValue => null
              case _ => throw new ElasticIndexException(s"${field.name} field not found.")
            }
            ElasticValTransform.sparkValue(field, value,
              nameToTF.get(field.name), info.options.timeZoneId,
              ec.dateTypeFormats,
              Some(info.options.dateTypeFormats))
          case None => throw new ElasticIndexException("WTF? The field not in columns")
        }
      })
      row
    }
  }
}

object TaskCancelHandler extends MyLogging {

  private val taskMap = TrieMap[String, (Cancellable, TaskCancelHolder, TaskContext)]()

  class TaskCancelHolder(val queryId: String, val taskContext: TaskContext) extends CancellableHolder {

    override def setCancellable(c: Cancellable): Unit = {
      log.debug(s"set cancellable for query $queryId")
      taskMap(queryId) = (c, this, taskContext)
    }

    @volatile var wasCancelTriggered = false
  }

  def registerQueryId(queryId: String, taskContext: TaskContext): TaskCancelHolder = {
    log.debug(s"register query $queryId")
    new TaskCancelHolder(queryId, taskContext)
  }

  def clearQueryId(queryId: String) = taskMap.remove(queryId)

  val sec5: Long = 5 * 1000

  object cancelCheckThread extends Runnable with MyLogging {

    def run(): Unit = {
      while (true) {
        Thread.sleep(sec5)
        log.debug("cancelThread woke up")
        var canceledTasks: Seq[String] = Seq()  // queryId list
        taskMap.foreach {
          case (queryId, (request, taskCancelHolder: TaskCancelHolder, taskContext)) =>
            log.debug(s"checking task stateId = ${taskContext.stageId()}, " +
              s"partitionId = ${taskContext.partitionId()}, " +
              s"isInterrupted = ${taskContext.isInterrupted()}")
            if (taskContext.isInterrupted()) {
              try {
                taskCancelHolder.wasCancelTriggered = true
                request.cancel()
                log.info(s"aborted http request for query $queryId: $request")
                canceledTasks = canceledTasks :+ queryId
              } catch {
                case e: Throwable => log.warn(s"failed to abort http request: $request")
              }
            }
        }
        canceledTasks.foreach(clearQueryId)
      }
    }
  }

  val t = new Thread(cancelCheckThread)
  t.setName("ElasticRDD-TaskCancelCheckThread")
  t.setDaemon(true)
  t.start()
}

object ElasticValTransform {

  object LongTimeTransformer {

    def unapply(timeFormat: (String, Option[String])): Option[Long] = {
      val time = timeFormat._1
      val format = timeFormat._2
      format match {
        case Some(fmt) =>
          Try {
            DateTime.parse(time, DateTimeFormat.forPattern(fmt))
          } match {
            case Success(dt) => Some(dt.getMillis)
            case Failure(_) => None
          }
        case _ => None
      }
    }
  }

  private val timeFormats = List[String](
    "yyyy-MM-dd",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ssZ",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
  )

  def timeToLong(time: String, format: Option[String] = None): Option[Long] = {
    (time, format) match {
      case LongTimeTransformer(longTime) => Some(longTime)
      case _ =>
        timeFormats.map(f => (time, Some(f))).collectFirst {
          case LongTimeTransformer(longTime) => longTime
        }
    }
  }

  /**
   * Just date type in es will map to timestampType in Spark,
   * so just the String/Long type of the elasticVal is valid.
   */
  private[this] val toTSWithTZAdj = (elasticVal: Any, tz: String,
                                     dateTypeFormats: Option[Seq[String]],
                                     defaultDateTypeFormats: Option[Seq[String]]) => {
    val evLong = toTS(elasticVal, tz, dateTypeFormats, defaultDateTypeFormats)

    // microsecond
    new DateTime(evLong / 1000, DateTimeZone.forID(tz)).getMillis * 1000.asInstanceOf[SQLTimestamp]
  }

  /**
   * Just date type in es will map to timestampType in Spark,
   * so just the String/Long type of the elasticVal is valid.
   */
  private[this] val toTS = (elasticVal: Any, tz: String,
                            dateTypeFormats: Option[Seq[String]],
                            defaultDateTypeFormats: Option[Seq[String]]) => {

    val formats: Seq[String] = defaultDateTypeFormats match {
      case Some(fmt1) =>
        dateTypeFormats match {
          case Some(fmt2) => (fmt1 ++ fmt2).distinct
          case None => fmt1
        }
      case None => throw new ElasticIndexException("WTF? Have no default date type formats?")
    }

    formats.collectFirst {
      case "strict_date_optional_time" | "date_optional_time" if elasticVal.isInstanceOf[String] =>
        timeToLong(elasticVal.asInstanceOf[String]) match {
          case Some(longTime) => longTime * 1000
          case _ => throw new ElasticIndexException("Unsupported date type format.")
        }
      case "epoch_millis" if elasticVal.isInstanceOf[Long] =>
        elasticVal.asInstanceOf[Long] * 1000
      case "epoch_second" if elasticVal.isInstanceOf[Int] =>
        elasticVal.asInstanceOf[Int] * 1000 * 1000
    } match {
      case Some(longTime) => longTime
      case _ => throw new ElasticIndexException("Unsupported date type format.")
    }
  }

  private[this] val toString = (elasticVal: Any, tz: String,
                                dateTypeFormats: Option[Seq[String]],
                                defaultDateTypeFormats: Option[Seq[String]]) => {
    UTF8String.fromString(elasticVal.toString)
  }

  private[this] val toInt = (elasticVal: Any, tz: String,
                             dateTypeFormats: Option[Seq[String]],
                             defaultDateTypeFormats: Option[Seq[String]]) => {
    if (elasticVal.isInstanceOf[Double]) {
      elasticVal.asInstanceOf[Double].toInt
    } else if (elasticVal.isInstanceOf[BigInt]) {
      elasticVal.asInstanceOf[BigInt].toInt
    } else if (elasticVal.isInstanceOf[String]) {
      elasticVal.asInstanceOf[String].toInt
    } else if (elasticVal.isInstanceOf[Integer]) {
      elasticVal.asInstanceOf[Integer].toInt
    } else elasticVal
  }

  private[this] val toLong = (elasticVal: Any, tz: String,
                              dateTypeFormats: Option[Seq[String]],
                              defaultDateTypeFormats: Option[Seq[String]]) => {
    if (elasticVal.isInstanceOf[Double]) {
      elasticVal.asInstanceOf[Double].toLong
    } else if (elasticVal.isInstanceOf[BigInt]) {
      elasticVal.asInstanceOf[BigInt].toLong
    } else if (elasticVal.isInstanceOf[String]) {
      elasticVal.asInstanceOf[String].toLong
    } else if (elasticVal.isInstanceOf[Int]) {
      elasticVal.asInstanceOf[Int].toLong
    } else if (elasticVal.isInstanceOf[Integer]) {
      elasticVal.asInstanceOf[Integer].toLong
    } else elasticVal
  }

  private[this] val toFloat = (elasticVal: Any, tz: String,
                               dateTypeFormats: Option[Seq[String]],
                               defaultDateTypeFormats: Option[Seq[String]]) => {
    if (elasticVal.isInstanceOf[Double]) {
      elasticVal.asInstanceOf[Double].toFloat
    } else if (elasticVal.isInstanceOf[BigInt]) {
      elasticVal.asInstanceOf[BigInt].toFloat
    } else if (elasticVal.isInstanceOf[String]) {
      elasticVal.asInstanceOf[String].toFloat
    } else if (elasticVal.isInstanceOf[Int]) {
      elasticVal.asInstanceOf[Int].toFloat
    } else if (elasticVal.isInstanceOf[Integer]) {
      elasticVal.asInstanceOf[Integer].toFloat
    } else elasticVal
  }

  private[this] val tfMap: Map[String, (Any, String, Option[Seq[String]], Option[Seq[String]]) => Any] = {
    Map[String, (Any, String, Option[Seq[String]], Option[Seq[String]]) => Any](
      "toTSWithTZAdj" -> toTSWithTZAdj,
      "toTS" -> toTS,
      "toString" -> toString,
      "toInt" -> toInt,
      "toLong" -> toLong,
      "toFloat" -> toFloat
    )
  }

  def defaultValueConversion(f: StructField, elasticVal: Any): Any = f.dataType match {
    case TimestampType if elasticVal.isInstanceOf[Double] =>
      elasticVal.asInstanceOf[Double].longValue()
    case StringType if elasticVal != null => UTF8String.fromString(elasticVal.toString)
    case LongType if elasticVal.isInstanceOf[BigInt] =>
      elasticVal.asInstanceOf[BigInt].longValue()
    case LongType if elasticVal.isInstanceOf[Integer] =>
      elasticVal.asInstanceOf[Integer].longValue()
    case BinaryType if elasticVal.isInstanceOf[String] =>
      Base64Variants.getDefaultVariant.decode(elasticVal.asInstanceOf[String])
    case _ => elasticVal

  }

  def sparkValue(f: StructField, elasticVal: Any, tfName: Option[String], tz: String,
                 dateTypeFormats: Option[Seq[String]],
                 defaultDateTypeFormats: Option[Seq[String]]): Any = {
    tfName match {
      case Some(tf) if tfMap.contains(tf) && elasticVal != null =>
        tfMap(tf)(elasticVal, tz, dateTypeFormats, defaultDateTypeFormats)
      case _ => defaultValueConversion(f, elasticVal)
    }
  }

  def getTFName(sparkDT: DataType, adjForTZ: Boolean = false): String = sparkDT match {
    case TimestampType if adjForTZ => "toTSWithTZAdj"
    case TimestampType => "toTS"
    case StringType if !adjForTZ => "toString"
    case ShortType if !adjForTZ => "toInt"
    case LongType => "toLong"
    case FloatType => "toFloat"
    case _ => ""
  }
}
