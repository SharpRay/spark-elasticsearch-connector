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

    val `type`: Option[String]  = parameters.get(ELASTIC_TYPE)

    val poolMaxConnectionsPerRoute: Int = parameters.getOrElse(CONN_POOL_MAX_CONNECTIONS_PER_ROUTE,
      DEFAULT_CONN_POOL_MAX_CONNECTIONS_PER_ROUTE).toInt

    val poolMaxConnections: Int = parameters.getOrElse(CONN_POOL_MAX_CONNECTIONS,
      DEFAULT_CONN_POOL_MAX_CONNECTIONS).toInt

    val cacheIndexMappings: Boolean = parameters.getOrElse(CACHE_INDEX_MAPPINGS,
      DEFAULT_CACHE_INDEX_MAPPINGS).toBoolean

    val skipUnknownTypeFields: Boolean = parameters.getOrElse(SKIP_UNKNOWN_TYPE_FIELDS,
      DEFAULT_SKIP_UNKNOWN_TYPE_FIELDS).toBoolean

    val debugTransformations: Boolean = parameters.getOrElse(DEBUG_TRANSFORMATIONS,
      DEFAULT_DEBUG_TRANSFORMATIONS).toBoolean

    val timeZoneId: String = parameters.getOrElse(TIME_ZONE_ID, DEFAULT_TIME_ZONE_ID)

    val dateTypeFormats: Seq[String] = parameters.getOrElse(DATE_TYPE_FORMAT, DEFAULT_DATE_TYPE_FORMAT)
      .split("\\|\\|").toSeq

    val nullFillNonexistentFieldValue = parameters.getOrElse(NULL_FILL_NONEXISTENT_FIELD_VALUE,
      DEFAULT_NULL_FILL_NONEXISTENT_FIELD_VALUE).toBoolean

    val defaultLimit = parameters.getOrElse(DEFAULT_LIMIT, DEFAULT_DEFAULT_LIMIT).toInt

    val elasticOptions = ElasticOptions(host,
      index,
      `type`,
      poolMaxConnectionsPerRoute,
      poolMaxConnections,
      cacheIndexMappings,
      skipUnknownTypeFields,
      debugTransformations,
      timeZoneId,
      dateTypeFormats,
      nullFillNonexistentFieldValue,
      defaultLimit)

    val elasticRelationInfo = ElasticMetadataCache.elasticRelation(elasticOptions)

    val elasticRelation = ElasticRelation(elasticRelationInfo, None)(sqlContext)

//    addPhysicalRules(sqlContext, elasticOptions)

    elasticRelation
  }

  /**
   * There are 3 places to initialize a [[BaseRelation]] object by calling
   * the `resolveRelation` method of [[org.apache.spark.sql.execution.datasources.DataSource]]:
   *
   *   1. In the `run(sparkSession: SparkSession)` method in
   *      [[org.apache.spark.sql.execution.command.CreateDataSourceTableCommand]]
   *      when executing sql "create table using ...";
   *   2. In the `load(paths: String*)` method in [[org.apache.spark.sql.DataFrameReader]]
   *      when calling "spark.read.format(org.rzlabs.druid).load()";
   *   3. In the `load` method of the LoadingCache object "cachedDataSourceTables" in
   *      [[org.apache.spark.sql.hive.HiveMetastoreCatalog]] which called from the root
   *      method of `apply` in [[org.apache.spark.sql.catalyst.analysis.Analyzer.ResolveRelations]]
   *      which belongs to "resolution" rule batch in the logical plan analyzing phase of the
   *      execution of "select ...".
   *
   * None of the 3 cases generates [[org.apache.spark.sql.execution.SparkPlan]] in DataFrame's
   * `queryExecution` of [[org.apache.spark.sql.execution.QueryExecution]], so we can
   * add druid-related physical rules in the `resolveRelation` method in [[DefaultSource]].
   *
   * @param sqlContext
   * @param options
   */
//  private def addPhysicalRules(sqlContext: SQLContext, options: ElasticOptions) = {
//    rulesLock.synchronized {
//      if (!physicalRulesAdded) {
//        sqlContext.sparkSession.experimental.extraStrategies ++=
//          ElasticBaseModule.physicalRules(sqlContext, options)
//        physicalRulesAdded = true
//      }
//    }
//  }

}

object DefaultSource {

//  private val rulesLock = new Object

//  private var physicalRulesAdded = false

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

  val CACHE_INDEX_MAPPINGS = "cacheIndexMappings"
  val DEFAULT_CACHE_INDEX_MAPPINGS = "false"

  val SKIP_UNKNOWN_TYPE_FIELDS = "skipUnknownTypeFields"
  val DEFAULT_SKIP_UNKNOWN_TYPE_FIELDS = "false"

  val DEBUG_TRANSFORMATIONS = "debugTransformations"
  val DEFAULT_DEBUG_TRANSFORMATIONS = "false"

  val TIME_ZONE_ID = "timeZoneId"
  val DEFAULT_TIME_ZONE_ID = "GMT"

  val DATE_TYPE_FORMAT = "dateTypeFormat"
  val DEFAULT_DATE_TYPE_FORMAT = "strict_date_optional_time||epoch_millis"

  val NULL_FILL_NONEXISTENT_FIELD_VALUE = "nullFillNonexistentFieldValue"
  val DEFAULT_NULL_FILL_NONEXISTENT_FIELD_VALUE = "false"

  val DEFAULT_LIMIT = "defaultLimit"
  val DEFAULT_DEFAULT_LIMIT = Int.MaxValue.toString
}
