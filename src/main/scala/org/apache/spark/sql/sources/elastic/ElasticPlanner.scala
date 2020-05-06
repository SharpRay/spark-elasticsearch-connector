package org.apache.spark.sql.sources.elastic

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.rzlabs.elastic.ElasticQueryBuilder
import org.rzlabs.elastic.client.ConnectionManager
import org.rzlabs.elastic.metadata.ElasticOptions

class ElasticPlanner(val sqlContext: SQLContext, val options: ElasticOptions)
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

  def apply(sqlContext: SQLContext, options: ElasticOptions) = {
    val planner = new ElasticPlanner(sqlContext, options)
    ConnectionManager.init(options)
    planner
  }
}
