package org.rzlabs.elastic

import java.util.concurrent.atomic.AtomicLong

import org.rzlabs.elastic.metadata.{ElasticRelationColumn, ElasticRelationInfo}

import scala.collection.mutable.{Map => MMap}

case class ElasticQueryBuilder(relationInfo: ElasticRelationInfo,
                               referenceElasticColumns: MMap[String, ElasticRelationColumn] = MMap(),
                               projectionAliasMap: Map[String, String] = Map(),
                               curId: AtomicLong = new AtomicLong(-1),
                               hasUnpushedProjections: Boolean = false,
                               hasUnpushedFilters: Boolean = false) {

  def elasticCoumn(name: String): Option[ElasticRelationColumn] = {
    relationInfo.indexInfo.columns.get(projectionAliasMap.getOrElse(name, name)).map {
      elasticColumn =>
        val elasticRelationColumn = ElasticRelationColumn(name, Some(elasticColumn))
        referenceElasticColumns(name) = elasticRelationColumn
        elasticRelationColumn
    }
  }

  def addAlias(alias: String, col: String) = {
    val colName = projectionAliasMap.getOrElse(col, col)
    this.copy(projectionAliasMap = projectionAliasMap + (alias -> colName))
  }

  /**
   * From "alias-1" to "alias-N"
   * @return
   */
  def nextAlias: String = s"alias${curId.getAndDecrement()}"
}

object ElasticQueryBuilder {
  def apply(relationInfo: ElasticRelationInfo): ElasticQueryBuilder =
    new ElasticQueryBuilder(relationInfo)
}
