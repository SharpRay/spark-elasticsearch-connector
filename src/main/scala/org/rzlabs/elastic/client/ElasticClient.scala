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
import org.rzlabs.elastic.{ElasticColumn, ElasticIndex, ElasticIndexException}
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

  def post(url: String,
                     payload: ObjectNode,
                     reqHeaders: Map[String, String] = null): String = {
    perform(url, postRequest _, payload, reqHeaders);
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
  def mappings(index: String, `type`: String = null) = {
    val url = s"http://$host:$port/${index}/_mappings"
    val resp: String = get(url)
    logWarning(s"The json response of '_mappings' query: \n$resp")
    val mappingsResp: Map[String, IndexMappings] = jsonMapper.readValue(resp,
      new TypeReference[Map[String, IndexMappings]] {})
    if (`type` == null) {
      ElasticIndex(index, mappingsResp.get(index).get.mappings.head._1,
        mappingsResp.get(index).get.mappings.head._2.properties.map(prop => {
          (prop._1, ElasticColumn(prop._1, prop._2))
        })
      )
    } else {
      ElasticIndex(index, `type`,
        mappingsResp.get(index).get.mappings.get(`type`).get.properties.map(prop => {
          (prop._1, ElasticColumn(prop._1, prop._2))
        })
      )
    }
  }

}

object ElasticClient {

  val HOST = """([^:]*):(\d*)""".r

  def hostPort(s: String): (String, Int) = {
    val HOST(h, p) = s
    (h, p.toInt)
  }
}
