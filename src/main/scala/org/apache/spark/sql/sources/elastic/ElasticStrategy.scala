package org.apache.spark.sql.sources.elastic

import org.apache.spark.sql.catalyst.elastic.execution.CollectOffsetExec
import org.apache.spark.sql.catalyst.elastic.plans.logical.Offset
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, Divide, Expression, IntegerLiteral, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ExprUtil
import org.apache.spark.sql.{MyLogging, Strategy}
import org.rzlabs.elastic._
import org.rzlabs.elasticsearch.{ElasticAttribute, ElasticQuery, ElasticRelation}

private[sql] class ElasticStrategy(val planner: ElasticPlanner) extends Strategy
  with MyLogging {

  override def apply(lp: LogicalPlan): Seq[SparkPlan] = {

    val plan: Seq[SparkPlan] = for (eqb <- planner.plan(null, lp)) yield {
      if (eqb.aggregateOp.isDefined) {
        // TODO: aggregation implementation
        null
      } else if (eqb.hasOffsetAggregate) {
        offsetPlan(lp)
      } else {
        searchPlan(eqb, lp)
      }
    }

    plan.filter(_ != null).toList
    if (plan.size < 2) plan else Seq(UnionExec(plan))
  }

  private def searchPlan(eqb: ElasticQueryBuilder, lp: LogicalPlan): SparkPlan = {
    lp match {
      case ReturnAnswer(child) => searchPlan(eqb, child)
//      // TODO: remove to other user defined SparkStrategy implementation
//      case Offset(offsetExpr, aggr @ Aggregate(_, _, _)) =>
//        offsetPlan(offsetExpr, None, aggr)
//      // TODO: remove to other user defined SparkStrategy implementation
//      case Offset(offsetExpr, Limit(limitExpr, aggr @ Aggregate(_, _, _))) =>
//        println("offset -> limit -> aggregate!!!!!!!!!!!!!!!!!!")
//        offsetPlan(offsetExpr, Some(limitExpr), aggr)

      case Sort(_, _, child) => searchPlan(eqb, child)
      case Offset(_, child) => searchPlan(eqb, child)
      case Limit(_, child) => searchPlan(eqb, child)
      case Project(projectList, _) => searchPlan(eqb, projectList)
      case LogicalRelation(_, output, _, _) => searchPlan(eqb, output)
      case _ => null
    }
  }

  // TODO: remove to other user defined SparkStrategy implementation
  private def offsetPlan(lp: LogicalPlan): SparkPlan = lp match {
    case ReturnAnswer(child) => offsetPlan(child)
    case Offset(IntegerLiteral(offset), Limit(IntegerLiteral(limit), aggr @ Aggregate(_, _, _))) =>
      CollectOffsetExec(offset, limit, planLater(aggr))
    case Offset(IntegerLiteral(offset), Limit(IntegerLiteral(limit), sort @ Sort(_, _, Aggregate(_, _, _)))) =>
      CollectOffsetExec(offset, limit, planLater(sort))
    case Offset(IntegerLiteral(offset), aggr @ Aggregate(_, _, _)) =>
      CollectOffsetExec(offset, -1, planLater(aggr))
    case Offset(IntegerLiteral(offset), sort @ Sort(_, _, Aggregate(_, _, _))) =>
      CollectOffsetExec(offset, -1, planLater(sort))
    case _ => null
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

//    val qrySpec: QuerySpec = SearchQuerySpec(planner.options.index,
//      planner.options.`type`,
//      columns,
//      eqb1.filterSpec,
//      eqb1.offsetSpec.map(_.from),
//      eqb1.limitSpec.map(_.size),
//      eqb1.sortSpec.map(_.sort))

    val qrySpec: QuerySpec = SearchQuerySpec(
      eqb1.relationInfo.options.index,
      eqb1.relationInfo.options.`type`,
      columns,
      eqb1.filterSpec,
      eqb1.offsetSpec.map(_.from),
      Some(eqb1.limitSpec.map(_.size).getOrElse(eqb1.relationInfo.options.defaultLimit)),
      eqb1.sortSpec.map(_.sort))

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
