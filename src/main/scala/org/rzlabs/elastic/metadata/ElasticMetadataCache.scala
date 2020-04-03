package org.rzlabs.elastic.metadata

import org.apache.spark.sql.MyLogging
import org.rzlabs.elastic.ElasticIndex
import org.rzlabs.elastic.client.{ClusterStatus, ElasticClient}

import scala.collection.mutable.{Map => MMap}

case class ElasticClusterInfo(host: String,
                              clusterStatus: ClusterStatus,
                              indexes: MMap[String, ElasticIndex])

trait ElasticMetadataCache {

  def getIndexInfo(options: ElasticOptions): ElasticIndex
}

trait ElasticRelationInfoCache {

  self: ElasticMetadataCache =>

  def elasticRelation(options: ElasticOptions) = {
    val elasticIndex = getIndexInfo(options)
    ElasticRelationInfo(elasticIndex.name, elasticIndex, options)
  }
}

object ElasticMetadataCache extends ElasticMetadataCache with MyLogging with ElasticRelationInfoCache {

  private[metadata] val cache: MMap[String, ElasticClusterInfo] = MMap()

  def getClusterInfo(options: ElasticOptions) = {
    val host = options.host
    cache.synchronized {
      if (options.cacheIndexMappings && cache.contains(host)) {
        cache(host)
      } else {
        val elasticClient = new ElasticClient(host)
        val clusterStatus = elasticClient.clusterStatus
        val clusterInfo = new ElasticClusterInfo(host, clusterStatus,
          MMap[String, ElasticIndex]())
        cache(host) = clusterInfo
        logInfo(s"Loading elasticsearch cluster info with host ${host}")
        clusterInfo
      }
    }
  }

  override def getIndexInfo(options: ElasticOptions): ElasticIndex = {
    val host = options.host
    val index = options.index
    val `type` = options.`type`
    val clusterInfo = getClusterInfo(options)
    clusterInfo.synchronized {
      if (clusterInfo.indexes.contains(index)) {
        clusterInfo.indexes(index)
      } else {
        val elasticClient = new ElasticClient(host)
        val elasticIndex: ElasticIndex = elasticClient.mappings(index, `type`,
          options.skipUnknownTypeFields)
        elasticIndex.columns.foreach(c => {
          println(c._1)
          println(c._2)
        })
        clusterInfo.indexes(index) = elasticIndex
        logInfo(s"Elasticsearch index info for ${index} is loaded.")
        elasticIndex
      }
    }
  }
}