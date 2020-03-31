package org.rzlabs.elastic.metadata

import org.apache.spark.sql.MyLogging

import scala.collection.mutable.{Map => MMap}

case class ElasticClusterInfo()

trait ElasticMetadataCache {

  def getIndexInfo(options: ElasticOptions)
}

trait ElasticRelationInfoCache {

  self: ElasticMetadataCache =>

  def elasticRelation(options: ElasticOptions) = {
    getIndexInfo(options)

  }
}
object ElasticMetadataCache extends ElasticMetadataCache with MyLogging with ElasticRelationInfoCache {

  private[metadata] val cache: MMap[String, ElasticClusterInfo] = MMap()

  def getClusterInfo(options: ElasticOptions) = {
    val host = options.host
    cache.synchronized {
      if (cache.contains(host)) {
        cache(host)
      } else {

      }
    }
  }

  override def getIndexInfo(options: ElasticOptions): Unit = {

  }
}