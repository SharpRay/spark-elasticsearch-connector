package org.rzlabs.elastic.metadata

import org.rzlabs.elastic.{ElasticColumn, ElasticDataType}

case class ElasticRelationColumn(column: String,
                                 elasticColumn: Option[ElasticColumn]) {

  def hasDirectElasticColumn = elasticColumn.isDefined

  def isNotIndexedColumn = !hasDirectElasticColumn

  def dataType = {
    if (isNotIndexedColumn) {
      ElasticDataType.Unknown
    } else {
      elasticColumn.get.dataType
    }
  }

  def keywordField = {
    if (!isNotIndexedColumn) {
      elasticColumn.get.keywordFields match {
        case Some(keywordFields) if keywordFields.size != 0 =>
          Some(keywordFields(0))
        case _ => None
      }
    } else None
  }
}
