package org.apache.spark.sql.sources.elastic

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.{SparkPlan, UnionExec}
import org.apache.spark.sql.{MyLogging, Strategy}
import org.rzlabs.elastic.{ElasticDataType, ElasticQueryBuilder, QuerySpec, SearchQuerySpec}

private[sql] class ElasticStrategy(planner: ElasticPlanner) extends Strategy
  with MyLogging {

  override def apply(lp: LogicalPlan): Seq[SparkPlan] = {

    val plan: Seq[SparkPlan] = for (eqb <- planner.plan(null, lp)) yield {
      if (eqb.aggregateOp.isDefined) {
        // TODO: aggregation implementation
        null
      } else {
        searchPlan(eqb, lp)
      }
    }

    plan.filter(_ != null).toList
    if (plan.size < 2) plan else Seq(UnionExec(plan))
  }

  private def searchPlan(eqb: ElasticQueryBuilder, lp: LogicalPlan): SparkPlan = {
    lp match {
      case Project(projectList, _) => searchPlan(eqb, projectList)
      case _ => null
    }
  }

  private def searchPlan(eqb: ElasticQueryBuilder,
                       projectList: Seq[NamedExpression]): SparkPlan = {

    var eqb1 = eqb

    def addOutputAttributes(exprs: Seq[Expression]) = {
      for (na <- exprs;
           attr <- na.references;
           ec <- eqb.elasticCoumn(attr.name)) {
        eqb1 = eqb1.outputAttribute(attr.name, attr, attr.dataType,
          ElasticDataType.sparkDataType(ec.dataType), null)
      }
    }

    // Set outputAttrs with projectList
    addOutputAttributes(projectList)
    // Set outputAttrs with filters
    addOutputAttributes(eqb1.origFilter.toSeq)

    val elasticSchema = new ElasticSchema(eqb1)

    val columns = eqb1.referenceElasticColumns.values.map(_.column).toList

    val qrySpec: QuerySpec = SearchQuerySpec(planner.options.index,
      planner.options.`type`,
      columns,
      eqb1.filterSpec)


  }
}
