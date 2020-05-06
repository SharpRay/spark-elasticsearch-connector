package org.apache.spark.sql.sources.elastic

import org.apache.spark.sql.catalyst.plans.logical._
import org.rzlabs.elastic.Utils

trait LimitTransform {
  self: ElasticPlanner =>

  /**
   * ==Sort Rewrite:==
   * A '''Sort''' Operator is pushed down to ''Elasticsearch'' if all its __order expressions__
   * can be pushed down. An __order expression__ is pushed down if it is on an ''Expression''
   * that is already pushed to Elasticsearch, or if it is an [[Alias]] expression whose child
   * has been pushed to Elasticsearch.
   *
   * ==Limit Rewrite:==
   * A '''Limit''' Operator above a Sort is always pushed down to Elasticsearch.
   */
  val limitTransform: ElasticTransform = {
    /**
     * handle the case of [[ReturnAnswer]] wrapping a ''limit'' plan pattern, because
     * if we don't handle it here it gets transformed by the
     * [[org.apache.spark.sql.execution.SparkStrategies.SpecialLimits]] strategy.
     */
    case (eq, ReturnAnswer(child)) => limitTransform(eq, child)
    case (eqb, sort @ Sort(orderExprs, global, child: Aggregate)) =>
      // TODO: handle the Aggregate expression.
      Nil
    case (eqb, sort @ Limit(limitExpr, child: Sort)) =>
      // TODO: handle the Sort expression.
      val eqbs = plan(eqb, child).map { eqb =>
        val amt = limitExpr.eval(null).asInstanceOf[Int]
        eqb.limit(amt)
      }
      Utils.sequence(eqbs.toList).getOrElse(Nil)
    case (eqb, limit @ Limit(limitExpr, child)) =>
      val eqbs = plan(eqb, child).map { eqb =>
        val amt = limitExpr.eval(null).asInstanceOf[Int]
        eqb.limit(amt)
      }
      Utils.sequence(eqbs.toList).getOrElse(Nil)
    case (eqb, sort @ Limit(limitExpr, Project(projections,
      child @ Sort(_, _, aggChild: Aggregate)))) =>
      // TODO: handle the Aggregate expression.
      Nil
    case _ => Nil
  }
}

