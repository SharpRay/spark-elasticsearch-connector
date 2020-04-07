package org.rzlabs.elastic

import org.rzlabs.elastic.metadata.ElasticRelationInfo

case class Interval()

case class QueryIntervals(relationInfo: ElasticRelationInfo,
                          intervals: List[Interval] = Nil) {

  def get = intervals

}
