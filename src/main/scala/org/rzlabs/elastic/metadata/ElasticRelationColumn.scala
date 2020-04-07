package org.rzlabs.elastic.metadata

import org.rzlabs.elastic.ElasticColumn

case class ElasticRelationColumn(column: String,
                                 elasticColumn: Option[ElasticColumn])
