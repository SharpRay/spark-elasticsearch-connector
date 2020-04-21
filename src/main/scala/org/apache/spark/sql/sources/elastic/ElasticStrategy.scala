package org.apache.spark.sql.sources.elastic

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, Divide, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.{ProjectExec, RowDataSourceScanExec, SparkPlan, UnionExec}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ExprUtil
import org.apache.spark.sql.{MyLogging, Strategy}
import org.rzlabs.elastic._
import org.rzlabs.elasticsearch.{ElasticAttribute, ElasticQuery, ElasticRelation}

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

    val elasticQuery = ElasticQuery(qrySpec, Some(elasticSchema.elasticAttributes))

    def postElasticStep(plan: SparkPlan): SparkPlan = {
      // TODO: has post step?
      plan
    }

    def buildProjectList(eqb: ElasticQueryBuilder, elasticSchema: ElasticSchema): Seq[NamedExpression] = {
      buildProjectionList(projectList, elasticSchema)
    }

    buildPlan(eqb1, elasticSchema, elasticQuery, planner, postElasticStep _, buildProjectList _)
  }

  private def buildPlan(eqb: ElasticQueryBuilder,
                        elasticSchema: ElasticSchema,
                        elasticQuery: ElasticQuery,
                        planner: ElasticPlanner,
                        postElasticStep: SparkPlan => SparkPlan,
                        buildProjectList: (ElasticQueryBuilder, ElasticSchema) => Seq[NamedExpression]
                       ): SparkPlan = {

    val elasticRelation = ElasticRelation(eqb.relationInfo, Some(elasticQuery))(planner.sqlContext)

    val fullAttributes = elasticSchema.schema
    val requiredColumnIndex = (0 until fullAttributes.size).toSeq

    val elasticSparkPlan = postElasticStep(
      RowDataSourceScanExec(fullAttributes,
        requiredColumnIndex,
        Set(),
        Set(),
        elasticRelation.buildInternalScan,
        elasticRelation,
        None)
    )

    if (elasticSparkPlan != null) {
      val projections = buildProjectList(eqb, elasticSchema)
      ProjectExec(projections, elasticSparkPlan)
    } else null
  }

  private def buildProjectionList(origExpressions: Seq[NamedExpression],
                                  elasticSchema: ElasticSchema): Seq[NamedExpression] = {
    val elasticPushDownExprMap: Map[Expression, ElasticAttribute] =
      elasticSchema.pushedDownExprToElasticAttr
    val avgExpressions = elasticSchema.avgExpressions

    origExpressions.map { ne => ExprUtil.transformReplace(ne, {
      case e: Expression if avgExpressions.contains(e) =>
        val (sumAlias, cntAlias) = avgExpressions(e)
        val sumAttr: ElasticAttribute = elasticSchema.elasticAttrMap(sumAlias)
        val cntAttr: ElasticAttribute = elasticSchema.elasticAttrMap(cntAlias)
        Cast(Divide(
          Cast(AttributeReference(sumAttr.name, sumAttr.dataType)(sumAttr.exprId), DoubleType),
          Cast(AttributeReference(cntAttr.name, cntAttr.dataType)(cntAttr.exprId), DoubleType)
        ), e.dataType)
      case ae: AttributeReference if elasticPushDownExprMap.contains(ae) &&
        elasticPushDownExprMap(ae).dataType != ae.dataType =>
        val ea = elasticPushDownExprMap(ae)
        Alias(Cast(AttributeReference(ea.name, ea.dataType)(ea.exprId), ae.dataType), ea.name)(ea.exprId)
      case ae: AttributeReference if elasticPushDownExprMap.contains(ae) &&
        elasticPushDownExprMap(ae).name != ae.name =>
        val ea = elasticPushDownExprMap(ae)
        Alias(AttributeReference(ea.name, ea.dataType)(ea.exprId), ea.name)(ea.exprId)
      case ae: AttributeReference if elasticPushDownExprMap.contains(ae) => ae
      case e: Expression if elasticPushDownExprMap.contains(e) &&
        elasticPushDownExprMap(e).dataType != e.dataType =>
        val ea = elasticPushDownExprMap(e)
        Cast(AttributeReference(ea.name, ea.dataType)(ea.exprId), e.dataType)
      case e: Expression if elasticPushDownExprMap.contains(e) =>
        val ea = elasticPushDownExprMap(e)
        AttributeReference(ea.name, ea.dataType)(ea.exprId)
    }).asInstanceOf[NamedExpression]}
  }
}
