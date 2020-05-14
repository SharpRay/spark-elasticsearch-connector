package org.apache.spark.sql.sources.elastic

import org.apache.spark.sql.MyLogging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.rzlabs.elastic.ElasticQueryBuilder

abstract class ElasticTransforms extends MyLogging with ProjectFilterTransform with LimitTransform {

  self: ElasticPlanner => // ElasticTransforms can only be inherited by ElasticPlanner

  type ElasticTransform = Function[(Seq[ElasticQueryBuilder], LogicalPlan), Seq[ElasticQueryBuilder]]

  case class ORTransform(t1: ElasticTransform, t2: ElasticTransform) extends ElasticTransform {

    def apply(p: (Seq[ElasticQueryBuilder], LogicalPlan)): Seq[ElasticQueryBuilder] = {
      val r = t1(p._1, p._2)
      if (r.size > 0) {
        r
      } else {
        t2(p._1, p._2)
      }
    }
  }

  case class DebugTransform(transformName: String, t: ElasticTransform) extends ElasticTransform {

    def apply(p: (Seq[ElasticQueryBuilder], LogicalPlan)): Seq[ElasticQueryBuilder] = {
      val eqb = p._1
      val lp = p._2
      val reqb = t((eqb, lp))
      if (reqb.exists(eqb => eqb.relationInfo != null && eqb.relationInfo.options.debugTransformations)) {
        logInfo(s"$transformName transform invoked:\n" +
          s"Input ElasticQueryBuilders: $eqb\n" +
          s"Input LogicalPlan: $lp\n" +
          s"Output ElasticQueryBuilders: $reqb\n")
      }
      reqb
    }
  }

  case class TransformHolder(t: ElasticTransform) {

    def or(t2: ElasticTransform): ElasticTransform = ORTransform(t, t2)

    def debug(name: String): ElasticTransform = DebugTransform(name, t)
  }

  /**
   * Convert an object's type from ElasticTransform to TransformHolder implicitly.
   * So we call call "transform1.or(transform2)" or "transform1.debug(transformName)".
   * @param t The input ElasticTransform object.
   * @return The converted TransformHolder object
   */
  implicit def transformToHolder(t: ElasticTransform) = TransformHolder(t)

  def debugTransform(msg: => String): Unit = logInfo(msg)
}
