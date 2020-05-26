package org.rzlabs.elastic.client

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.http.HttpEntity
import org.apache.http.client.methods._
import org.apache.http.concurrent.Cancellable
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.MyLogging
import org.apache.spark.sql.sources.elastic.CloseableIterator
import org.rzlabs.elastic._
import org.rzlabs.elastic.metadata.ElasticOptions
import org.fasterxml.jackson.databind.ObjectMapper._

import scala.util.Try

object ConnectionManager {

  @volatile private var initialized: Boolean = false

  lazy val pool = {
    val p = new PoolingHttpClientConnectionManager()
    p.setMaxTotal(40)
    p.setDefaultMaxPerRoute(8)
    p
  }

  def init(options: ElasticOptions): Unit = {
    if (!initialized) {
      init(options.poolMaxConnectionsPerRoute,
        options.poolMaxConnections)
      initialized = true
    }
  }

  def init(maxPerRoute: Int, maxTotal: Int): Unit = {
    if (!initialized) {
      pool.setMaxTotal(maxTotal)
      pool.setDefaultMaxPerRoute(maxPerRoute)
      initialized = true
    }
  }
}

/**
 * A mixin trait that relays [[Cancellable]] resources to
 * a [[CancellableHolder]].
 */
trait ElasticClientHttpExecutionAware extends HttpExecutionAware {

  val ch: CancellableHolder

  abstract override def isAborted: Boolean = super.isAborted

  abstract override def setCancellable(cancellable: Cancellable): Unit = {
    if (ch != null) {
      ch.setCancellable(cancellable)
    }
    super.setCancellable(cancellable)
  }
}

/**
 * Configure [[HttpGet]] to have the [[ElasticClientHttpExecutionAware]] trait,
 * so that [[Cancellable]] resources are relayed to the registered [[CancellableHolder]].
 * @param url The url the request take data from.
 * @param ch The registered CancellableHolder.
 */
class ElasticHttpGet(url: String, val ch: CancellableHolder)
  extends HttpGet(url) with ElasticClientHttpExecutionAware

/**
 * Configure [[HttpPost]] to have the [[ElasticClientHttpExecutionAware]] trait,
 * so that [[Cancellable]] resources are relayed to the registered [[CancellableHolder]].
 * @param url The url the request posted to.
 * @param ch The registered CancellableHolder.
 */
class ElasticHttpPost(url: String, val ch: CancellableHolder)
  extends HttpPost(url) with ElasticClientHttpExecutionAware

/**
 * A mechanism to relay [[org.apache.http.concurrent.Cancellable]] resources
 * associated with the "http connection" of a "ElasticClient". This is used by
 * the [[org.rzlabs.elastic.TaskCancelHandler]] to capture the association
 * between "Spark Tasks" and "Cancellable" resources (connections).
 */
trait CancellableHolder {
  def setCancellable(c: Cancellable)
}

class ElasticClient(val host: String,
                    val port: Int) extends MyLogging {

  private var cancellableHolder: CancellableHolder = null

  def this(t: (String, Int)) = {
    this(t._1, t._2)
  }

  def this(s: String) = {
    this(ElasticClient.hostPort(s))
  }

  def setCancellableHolder(c: CancellableHolder) = {
    cancellableHolder = c
  }

  def httpClient: CloseableHttpClient = {
    val sTime = System.currentTimeMillis()
    val r = HttpClients.custom().setConnectionManager(ConnectionManager.pool).build()
    val eTime = System.currentTimeMillis()
    logDebug(s"Time to get httpClient: ${eTime - sTime}")
    logDebug("Pool Stats: {}", ConnectionManager.pool.getTotalStats)
    r
  }

  def release(resp: CloseableHttpResponse): Unit = {
    Try {
      if (resp != null) EntityUtils.consume(resp.getEntity)
    } recover {
      case e => logError("Error returning client to pool",
        ExceptionUtils.getStackTrace(e))
    }
  }

  def getRequest(url: String) = new ElasticHttpGet(url, cancellableHolder)
  def postRequest(url: String) = new ElasticHttpPost(url, cancellableHolder)

  protected def addHeaders(req: HttpRequestBase, reqHeaders: Map[String, String]): Unit = {
    if (reqHeaders != null) {
      reqHeaders.foreach(header => req.setHeader(header._1, header._2))
    }
  }

  @throws[ElasticIndexException]
  def perform(url: String,
                        reqType: String => HttpRequestBase,
                        payload: ObjectNode,
                        reqHeaders: Map[String, String]): String = {
    var resp: CloseableHttpResponse = null

    val tis: Try[String] = for {
      r <- Try {
        val req: CloseableHttpClient = httpClient
        val request = reqType(url)
        // Just HttpPost extends HttpEntityEnclosingRequestBase.
        // HttpGet extends HttpRequestBase.
        if (payload != null && request.isInstanceOf[HttpEntityEnclosingRequestBase]) {
          val input: HttpEntity =
            new StringEntity(jsonMapper.writeValueAsString(payload), ContentType.APPLICATION_JSON)
          request.asInstanceOf[HttpEntityEnclosingRequestBase].setEntity(input)
        }
        addHeaders(request, reqHeaders)
        resp = req.execute(request)
        resp
      }
      is <- Try {
        val status = r.getStatusLine.getStatusCode
        if (status >= 200 && status < 300) {
          if (r.getEntity != null) {
            IOUtils.toString(r.getEntity.getContent)
          } else {
            throw new ElasticIndexException(s"Unexpected response status: ${r.getStatusLine}")
          }
        } else {
          throw new ElasticIndexException(s"Unexpected response status: ${r.getStatusLine}")
        }
      }
    } yield is

    release(resp)
    tis.getOrElse(tis.failed.get match {
      case eie: ElasticIndexException => throw eie
      case e => throw new ElasticIndexException("Failed in communication with Elasticsearch", e)
    })
  }

  @throws[ElasticIndexException]
  def performQuery(url: String,
                   reqType: String => HttpRequestBase,
                   qrySpec: QuerySpec,
                   payload: ObjectNode,
                   reqHeaders: Map[String, String]): CloseableIterator[ResultRow] = {

    var resp: CloseableHttpResponse = null

    val enterTime = System.currentTimeMillis()
    var beforeExecTime = System.currentTimeMillis()
    var afterExecTime = System.currentTimeMillis()

    val iter: Try[CloseableIterator[ResultRow]] = for {
      r <- Try {
        val req: CloseableHttpClient = httpClient
        val request: HttpRequestBase = reqType(url)
        if (payload != null && request.isInstanceOf[HttpEntityEnclosingRequestBase]) {
          // HttpPost
          val input: HttpEntity = new StringEntity(jsonMapper.writeValueAsString(payload),
            ContentType.APPLICATION_JSON)
          request.asInstanceOf[HttpEntityEnclosingRequestBase].setEntity(input)
        }
        addHeaders(request, reqHeaders)
        beforeExecTime = System.currentTimeMillis()
        resp = req.execute(request)
        afterExecTime = System.currentTimeMillis()
        resp
      }
      iter <- Try {
        val status = r.getStatusLine.getStatusCode
        if (status >= 200 && status < 300) {
          qrySpec(r.getEntity.getContent, this, release(r))
        } else {
          throw new ElasticIndexException(s"Unexpected response status: ${r.getStatusLine} " +
            s"on $url for query: " +
            s"\n ${Utils.toPrettyJson(Right(payload))}")
        }
      }
    } yield iter

    val afterIterBuildTime = System.currentTimeMillis()
    log.debug(s"request $url: beforeExecTime = ${beforeExecTime - enterTime}, " +
      s"execTime = ${afterExecTime - beforeExecTime}, " +
      s"iterBuildTime = ${afterIterBuildTime - afterExecTime}")
    iter.getOrElse {
      release(resp)
      iter.failed.get match {
        case ie: ElasticIndexException => throw ie
        case e => throw new ElasticIndexException("Failed in communication with Elasticsearch: ", e)
      }
    }
  }

  def post(url: String,
                     payload: ObjectNode,
                     reqHeaders: Map[String, String] = null): String = {
    perform(url, postRequest _, payload, reqHeaders);
  }

  def postQuery(url: String, qrySpec: QuerySpec,
                payload: ObjectNode,
                reqHeaders: Map[String, String] = null): CloseableIterator[ResultRow] = {
    performQuery(url, postRequest _, qrySpec, payload, reqHeaders);
  }

  def get(url: String,
                    payload: ObjectNode = null,
                    reqHeaders: Map[String, String] = null): String = {
    perform(url, getRequest _, payload, reqHeaders)
  }

  def clusterStatus: ClusterStatus = {
    val url = s"http://$host:$port/_cluster/health"
    val is: String = get(url)
    jsonMapper.readValue(is, new TypeReference[ClusterStatus] {})
  }

  @throws[ElasticIndexException]
  def mappings(index: String, `type`: Option[String], skipUnknownTypeField: Boolean) = {
    val url = s"http://$host:$port/${index}/_mappings"
    val resp: String = get(url)
    logDebug(s"The json response of '_mappings' query: \n$resp")
    val mappingsResp: Map[String, IndexMappings] = jsonMapper.readValue(resp,
      new TypeReference[Map[String, IndexMappings]] {})

    val indexMappings: IndexMappings = mappingsResp.get(index).get
    val theType: String = if (`type`.isEmpty) {
      indexMappings.mappings.head._1
    } else {
      if (!indexMappings.mappings.contains(`type`.get)) {
        throw new ElasticIndexException(s"The type '${`type`}' do not exist.")
      }
      `type`.get
    }
    ElasticIndex(index, theType,
      indexMappings.mappings.get(theType).get.properties.map(prop => {
        if (!skipUnknownTypeField && prop._2.dataType == ElasticDataType.Unknown) {
          throw new ElasticIndexException(s"'${prop._1}' field mapping without type definition.")
        } else {
          (prop._1, ElasticColumn(prop._1, prop._2))
        }
      }).filter(_._2.property.dataType != ElasticDataType.Unknown)
    )
  }

  def executeQuery(qrySpec: QuerySpec): List[ResultRow] = {
    val payload: ObjectNode = jsonMapper.valueToTree(qrySpec)
    val r = post(url(qrySpec), payload)
    jsonMapper.readValue(r, new TypeReference[List[ResultRow]] {})
  }

  @throws[ElasticIndexException]
  def executeQueryAsStream(qrySpec: QuerySpec): CloseableIterator[ResultRow] = {
    val payload: ObjectNode = jsonMapper.valueToTree(qrySpec)
    postQuery(url(qrySpec), qrySpec, payload)
  }

  private def url(qrySpec: QuerySpec) = {
    val index = qrySpec.index
    val `type` = qrySpec.`type`.getOrElse("")
    s"http://${host}:${port}/${index}/${`type`}/_search"
  }
}

object ElasticClient {

  val HOST = """([^:]*):(\d*)""".r

  def hostPort(s: String): (String, Int) = {
    val HOST(h, p) = s
    (h, p.toInt)
  }
}
