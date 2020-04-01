package org.rzlabs.elastic

import org.apache.spark.sql.{MyLogging, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}
import org.rzlabs.elastic.metadata.{ElasticMetadataCache, ElasticOptions}
import org.rzlabs.elasticsearch.ElasticRelation

class DefaultSource extends RelationProvider with MyLogging {

  import DefaultSource._

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    val host: String = parameters.getOrElse(ELASTIC_HOST, "localhost:9200")

    val index: String = parameters.getOrElse(ELASTIC_INDEX,
      throw new ElasticIndexException(
        s"'$ELASTIC_INDEX' must be specified for Elasticsearch datasource."
      )
    )

    val `type`: String  = parameters.getOrElse(ELASTIC_TYPE, null)

    val poolMaxConnectionsPerRoute: Int = parameters.getOrElse(CONN_POOL_MAX_CONNECTIONS_PER_ROUTE,
      DEFAULT_CONN_POOL_MAX_CONNECTIONS_PER_ROUTE).toInt

    val poolMaxConnections: Int = parameters.getOrElse(CONN_POOL_MAX_CONNECTIONS,
      DEFAULT_CONN_POOL_MAX_CONNECTIONS).toInt

    val elasticOptions = ElasticOptions(host,
      index,
      `type`,
      poolMaxConnectionsPerRoute,
      poolMaxConnections)

    val elasticRelationInfo = ElasticMetadataCache.elasticRelation(elasticOptions)

    val elasticRelation = ElasticRelation(elasticRelationInfo)(sqlContext)

    elasticRelation
  }
}

object DefaultSource {

  val ELASTIC_HOST = "host"
  val ELASTIC_INDEX = "index"
  val ELASTIC_TYPE = "type"

  /**
   * The max simultaneous live connections.
   */
  val CONN_POOL_MAX_CONNECTIONS_PER_ROUTE = "maxConnectionsPerRoute"
  val DEFAULT_CONN_POOL_MAX_CONNECTIONS_PER_ROUTE = "20"

  /**
   * The max simultaneous live connections of the Elasticsearch cluster.
   */
  val CONN_POOL_MAX_CONNECTIONS = "maxConnections"
  val DEFAULT_CONN_POOL_MAX_CONNECTIONS = "100"
}
