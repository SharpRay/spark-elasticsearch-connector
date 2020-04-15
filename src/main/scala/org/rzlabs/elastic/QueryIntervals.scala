package org.rzlabs.elastic

import org.joda.time.{DateTime, Interval}
import org.rzlabs.elastic.metadata.ElasticRelationInfo

case class QueryIntervals(relationInfo: ElasticRelationInfo,
                          intervals: List[Interval] = Nil) {

  def get = intervals

  /**
   * - if this is the first queryInterval, add it.
   * - if the new Interval overlaps with the current QueryInterval set interval to the overlap.
   * - otherwise, the interval is an empty interval.
   * @param in
   * @return
   */
  private def add(in: Interval): QueryIntervals = {
    if (intervals.isEmpty) {
      // The first query interval. e.g., time < '2020-01-01T00:00:00'
      QueryIntervals(relationInfo, List(in))
    } else {
      // new interval overlaps old.
      val oldIn  = intervals.head
      if (oldIn.overlaps(in)) {
        // two interval overlaps.
        // e.g., time > '2019-01-01T00:00:00' and time < '2020-01-01T00:00:00'
        QueryIntervals(relationInfo, List(oldIn.overlap(in)))
      } else {
        // e.g., time > '2020-01-01T00:00:00' and time < '2019-01-01T00:00:00'
        val invalidIn = Interval.parse("2020-01-01/2020-01-01")
        QueryIntervals(relationInfo, List(invalidIn))
      }
    }
  }

  def ltCond(dt: DateTime): Option[QueryIntervals] = {
    Some(add(new Interval(new DateTime(Long.MinValue), dt)))
  }

  def lteCond(dt: DateTime): Option[QueryIntervals] = {
    // This because interval's end is excluded.
    Some(add(new Interval(new DateTime(Long.MinValue), dt.plus(1))))
  }

  def gtCond(dt: DateTime): Option[QueryIntervals] = {
    // This is because interval's start is included.
    Some(add(new Interval(dt.plus(1), new DateTime(Long.MaxValue))))
  }

  def gteCond(dt: DateTime): Option[QueryIntervals] = {
    Some(add(new Interval(dt, new DateTime(Long.MaxValue))))
  }
}
