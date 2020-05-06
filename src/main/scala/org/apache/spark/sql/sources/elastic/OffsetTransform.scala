package org.apache.spark.sql.sources.elastic

import org.apache.spark.sql.catalyst.elastic.plans.logical.Offset
import org.apache.spark.sql.catalyst.plans.logical.{Limit, ReturnAnswer, Sort}
import org.rzlabs.elastic.Utils

trait OffsetTransform {
  self: ElasticPlanner =>

  val offsetTransform: ElasticTransform = {
    case (eqb, ReturnAnswer(child)) => offsetTransform(eqb, child)
    case (eqb, offset @ Offset(offsetExpr, child)) =>
      println("child ======================= " + child)
      val eqbs = plan(eqb, child).map { eqb =>
        val offset = offsetExpr.eval(null).asInstanceOf[Int]
        println("offset =================== " + offset)
        eqb.offset(offset)
      }
      Utils.sequence(eqbs.toList).getOrElse(Nil)
    case _ => Nil
  }
}

