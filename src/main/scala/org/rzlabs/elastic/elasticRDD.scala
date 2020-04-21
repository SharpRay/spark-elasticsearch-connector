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
import org.joda.time.{DateTime, DateTimeZone}
import org.rzlabs.elastic.client.{CancellableHolder, ConnectionManager, ElasticClient, ResultRow}
import org.rzlabs.elastic.metadata.ElasticRelationInfo
import org.rzlabs.elasticsearch.ElasticQuery

import scala.collection.concurrent.TrieMap

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
        ElasticValTransform.sparkValue(field, r.event(field.name),
          nameToTF.get(field.name), info.options.timeZoneId)
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

  private[this] val toTSWithTZAdj = (elasticVal: Any, tz: String) => {
    val evLong = if (elasticVal.isInstanceOf[Double]) {
      elasticVal.asInstanceOf[Double].toLong
    } else if (elasticVal.isInstanceOf[BigInt]) {
      elasticVal.asInstanceOf[BigInt].toLong
    } else if (elasticVal.isInstanceOf[String]) {
      elasticVal.asInstanceOf[String].toLong
    } else if (elasticVal.isInstanceOf[Int]) {
      elasticVal.asInstanceOf[Int].toLong
    } else if (elasticVal.isInstanceOf[Integer]) {
      elasticVal.asInstanceOf[Integer].toLong
    } else {
      elasticVal
    }

    // microsecond
    new DateTime(evLong, DateTimeZone.forID(tz)).getMillis * 1000.asInstanceOf[SQLTimestamp]
  }

  private[this] val toTS = (elasticVal: Any, tz: String) => {
    if (elasticVal.isInstanceOf[Double]) {
      elasticVal.asInstanceOf[Double].toLong
    } else if (elasticVal.isInstanceOf[BigInt]) {
      elasticVal.asInstanceOf[BigInt].toLong
    } else if (elasticVal.isInstanceOf[Int]) {
      elasticVal.asInstanceOf[Int].toLong
    } else if (elasticVal.isInstanceOf[Integer]) {
      elasticVal.asInstanceOf[Integer].toLong
    } else elasticVal
  }

  private[this] val toString = (elasticVal: Any, tz: String) => {
    UTF8String.fromString(elasticVal.toString)
  }

  private[this] val toInt = (elasticVal: Any, tz: String) => {
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

  private[this] val toLong = (elasticVal: Any, tz: String) => {
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

  private[this] val toFloat = (elasticVal: Any, tz: String) => {
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

  private[this] val tfMap: Map[String, (Any, String) => Any] = {
    Map[String, (Any, String) => Any](
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

  def sparkValue(f: StructField, elasticVal: Any, tfName: Option[String], tz: String): Any = {
    tfName match {
      case Some(tf) if (tfMap.contains(tf) && elasticVal != null) => tfMap(tf)(elasticVal, tz)
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
