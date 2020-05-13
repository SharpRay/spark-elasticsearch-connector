package org.apache.spark.sql.sources.elastic

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.rzlabs.elastic.ElasticQueryBuilder

class ElasticPlanner(val sqlContext: SQLContext)
  extends ElasticTransforms with ProjectFilterTransform with LimitTransform with OffsetTransform {

  val transforms: Seq[ElasticTransform] = Seq(
    elasticRelationTransform.debug("elasticRelationTransform"),
    limitTransform.debug("limit"),
    offsetTransform.debug("offset")
  )

  def plan(eqb: Seq[ElasticQueryBuilder], plan: LogicalPlan): Seq[ElasticQueryBuilder] =
    transforms.view.flatMap(_(eqb, plan))
}

object ElasticPlanner {

  def apply(sqlContext: SQLContext) = {
    val planner = new ElasticPlanner(sqlContext)
    planner
  }
}
