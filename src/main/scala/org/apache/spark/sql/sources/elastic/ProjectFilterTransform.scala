package org.apache.spark.sql.sources.elastic

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.util.ExprUtil
import org.rzlabs.elastic.{ElasticQueryBuilder, SparkIntervalConditionExtractor}
import org.rzlabs.elasticsearch.ElasticRelation

trait ProjectFilterTransform {

  self: ElasticPlanner =>

  def translateProjectFilter(eqb: Option[ElasticQueryBuilder], projectList: Seq[NamedExpression],
                             filters: Seq[Expression], ignoreProjectList: Boolean = false,
                             joinAttrs: Set[String] = Set()): Seq[ElasticQueryBuilder] = {
    def eqb1 = if (ignoreProjectList) eqb else {
      projectList.foldLeft(eqb) {
        (leqb, e) => leqb.flatMap(projectExpression(_, e, joinAttrs, false))
      }
    }

    if (eqb1.isDefined) {
      // A predicate on the Date type dimension will be rewritten.
      val ice = new SparkIntervalConditionExtractor(eqb1.get)
      // For each filter generates a new ElasticQueryBuilder
      var oeqb = filters.foldLeft(eqb1) { (loeqb, filter) =>
        loeqb.flatMap { leqb =>

        }

      }
    }
  }

  def intervalFilterExpression(eqb: ElasticQueryBuilder, ice: SparkIntervalConditionExtractor,
                               filter: Expression): Option[ElasticQueryBuilder] = filter match {
    case ice(ic) =>
  }

  def projectExpression(eqb: ElasticQueryBuilder, projectExpr: Expression,
                        joinAttrs: Set[String] = Set(),
                        ignoreProjectList: Boolean = false): Option[ElasticQueryBuilder] = projectExpr match {
    case _ if ignoreProjectList => Some(eqb)
    case AttributeReference(nm, _, _, _) if eqb.elasticCoumn(nm).isDefined => Some(eqb)
    case AttributeReference(nm, _, _, _) if joinAttrs.contains(nm) => Some(eqb)
    case Alias(ar @ AttributeReference(nm1, _, _, _), nm) => {
      for (eqbc <- projectExpression(eqb, ar, joinAttrs, ignoreProjectList)) yield
        eqbc.addAlias(nm, nm1)
    }
    case _ => addUnpushedAttributes(eqb, projectExpr, false)
  }

  def addUnpushedAttributes(eqb: ElasticQueryBuilder, e: Expression,
                            isProjection: Boolean): Option[ElasticQueryBuilder] = {
    if (isProjection) {
      Some(eqb.copy(hasUnpushedProjections = true))
    } else {
      Some(eqb.copy(hasUnpushedFilters = true))
    }
  }

  val elasticRelationTransform: ElasticTransform = {
    case (_, PhysicalOperation(projectList, filters,
      l @ LogicalRelation(d @ ElasticRelation(info), _, _, _))) =>
      // This is the initial ElasticQueryBuilder which all transformations
      // are based on.
      val eqb: Option[ElasticQueryBuilder] = Some(ElasticQueryBuilder(info))
      val (newFilters, eqb1) = ExprUtil.simplifyConjPred(eqb.get, filters)

  }
}
