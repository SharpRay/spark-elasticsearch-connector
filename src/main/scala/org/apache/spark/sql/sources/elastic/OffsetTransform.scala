package org.apache.spark.sql.sources.elastic

import org.apache.spark.sql.catalyst.elastic.plans.logical.Offset
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Limit, ReturnAnswer, Sort}
import org.rzlabs.elastic.Utils

trait OffsetTransform {
  self: ElasticPlanner =>

  val offsetTransform: ElasticTransform = {
    case (eqb, ReturnAnswer(child)) => offsetTransform(eqb, child)
    case (eqb, Offset(_, Limit(_, Aggregate(_, _, child)))) =>
      val eqbs = plan(eqb, child).map { eqb =>
        eqb.offsetAggregate(true)
      }
      Utils.sequence(eqbs.toList).getOrElse(Nil)
    case (eqb, Offset(_, Limit(_, Sort(_, _, Aggregate(_, _, child))))) =>
      val eqbs = plan(eqb, child).map { eqb =>
        eqb.offsetAggregate(true)
      }
      Utils.sequence(eqbs.toList).getOrElse(Nil)
    case (eqb, Offset(_, Aggregate(_, _, child))) =>
      val eqbs = plan(eqb, child).map { eqb =>
        eqb.offsetAggregate(true)
      }
      Utils.sequence(eqbs.toList).getOrElse(Nil)
    case (eqb, Offset(_, Sort(_, _, Aggregate(_, _, child)))) =>
      val eqbs = plan(eqb, child).map { eqb =>
        eqb.offsetAggregate(true)
      }
      Utils.sequence(eqbs.toList).getOrElse(Nil)
    case (eqb, Offset(offsetExpr, child)) =>
      val eqbs = plan(eqb, child).map { eqb =>
        val offset = offsetExpr.eval(null).asInstanceOf[Int]
        eqb.offset(offset)
      }
      Utils.sequence(eqbs.toList).getOrElse(Nil)
    case _ => Nil
  }
}

