package org.apache.spark.sql.sources.elastic

import org.apache.spark.sql.catalyst.elastic.expressions.{Match, MatchPhrase}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.util.ExprUtil
import org.apache.spark.unsafe.types.UTF8String
import org.rzlabs.elastic._
import org.rzlabs.elastic.client.{IndexProperty, NestedProperty}
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
          columnFilterExpression(leqb, ice, filter).map(spec =>
            leqb.filterSpecification(spec))
        }
      }
      oeqb = oeqb.map { eqb2 =>
        eqb2.copy(origProjectList = eqb2.origProjectList.map(_ ++ projectList).orElse(Some(projectList)))
          .copy(origFilter = eqb2.origFilter.flatMap(f =>
            ExprUtil.and(filters :+ f)).orElse(ExprUtil.and(filters)))
      }
      oeqb.map(Seq(_)).getOrElse(Seq())
    } else Seq()
  }

//  def intervalFilterExpression(eqb: ElasticQueryBuilder, ice: SparkIntervalConditionExtractor,
//                               filter: Expression): Option[ElasticQueryBuilder] = filter match {
//    case ice(ic) => eqb.queryInterval(ic)
//    case _ => None
//  }

  def columnFilterExpression(eqb: ElasticQueryBuilder, ice: SparkIntervalConditionExtractor,
                             filter: Expression): Option[FilterSpec] = {

    (eqb, filter) match {
      case ValidElasticNativeComparison(filterSpec) => Some(filterSpec)
      case (eqb, filter) => filter match {
        case Or(e1, e2) =>
          Utils.sequence(
            List(columnFilterExpression(eqb, ice, e1),
              columnFilterExpression(eqb, ice, e2))).map(specs =>
            //BoolExpressionFilterSpec(ConjExpressionFilterSpec(should = specs)))
            BoolExpressionFilterSpec(ConjExpressionFilterSpec(
              mustOpt = None, mustNotOpt = None, shouldOpt = Some(specs), filterOpt = None)))
        case And(e1, e2) =>
          Utils.sequence(
            List(columnFilterExpression(eqb, ice, e1),
              columnFilterExpression(eqb, ice, e2))).map(specs =>
            //BoolExpressionFilterSpec(ConjExpressionFilterSpec(must = specs)))
            BoolExpressionFilterSpec(ConjExpressionFilterSpec(
              mustOpt = Some(specs), mustNotOpt = None, shouldOpt = None, filterOpt = None)))
        case In(AttributeReference(nm, _, _, _), vals: Seq[Expression]) =>
          for (ec <- eqb.elasticCoumn(nm)
               if vals.forall(_.isInstanceOf[Literal]))
            yield new TermsFilterSpec(ec.column, vals.map(_.asInstanceOf[Literal].value).toList)
        case InSet(AttributeReference(nm, _, _, _), vals: Set[Any]) =>
          for (ec <- eqb.elasticCoumn(nm))
            yield new TermsFilterSpec(ec.column, vals.toList)
        case IsNotNull(AttributeReference(nm, _, _, _)) =>
          for (ec <- eqb.elasticCoumn(nm))
            yield new ExistsFilterSpec(ec.column)
          // TODO: Global Filter operator
          for (ec <- eqb.elasticCoumn(nm) if ec.dataType == ElasticDataType.Nested)
            yield DummyFilterSpec()
        case IsNull(AttributeReference(nm, _, _, _)) =>
          for (ec <- eqb.elasticCoumn(nm) if ec.dataType != ElasticDataType.Nested)
            //yield BoolExpressionFilterSpec(
            //  ConjExpressionFilterSpec(mustNot = List(new ExistsFilterSpec(ec.column))))
            yield BoolExpressionFilterSpec(ConjExpressionFilterSpec(
              mustOpt = None, mustNotOpt = Some(List(new ExistsFilterSpec(ec.column))), shouldOpt = None, filterOpt = None))
        case Not(e) =>
          for (spec <- columnFilterExpression(eqb, ice, e))
            //yield BoolExpressionFilterSpec(ConjExpressionFilterSpec(mustNot = List(spec)))
            yield BoolExpressionFilterSpec(ConjExpressionFilterSpec(
              mustOpt = None, mustNotOpt = Some(List(spec)), shouldOpt = None, filterOpt = None))
        // TODO: What is NULL SCAN ???
        case Literal(null, _) => None
        // TODO: GroovyGenerator
        case _ => None
      }
    }
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

  object ValidElasticNativeComparison {

    private def findRootAttribute(field: Expression,
                                  pathItems: Seq[String]): (AttributeReference, Seq[String]) = field match {
      case e @ AttributeReference(nm, _, _, _) => (e, Seq(nm) ++ pathItems)
      case e @ GetStructField(_, _, Some(nm)) => findRootAttribute(e.child, Seq(nm) ++ pathItems)
      case _ =>
        throw new ElasticIndexException("WTF? The nested field's root is not type of AttributeReference!?")
    }

    private def findIndexProperty(eqb: ElasticQueryBuilder, pathItems: Seq[String]): Option[IndexProperty] = {
      pathItems match {
        case head :: tail =>
          val ec: ElasticColumn = eqb.elasticCoumn(head).flatMap(_.elasticColumn)
            .getOrElse(throw new ElasticIndexException(s"WTF? The column '${head}' not exist."))
          tail.foldLeft[Option[IndexProperty]](Some(ec.property)) { (oprop, item) =>
            oprop match {
              case Some(prop) if prop.isInstanceOf[NestedProperty] =>
                prop.asInstanceOf[NestedProperty].properties.get(item)
              case Some(_) => oprop
              case None => None
            }
          }
      }
    }

    def unapply(t: (ElasticQueryBuilder, Expression)): Option[FilterSpec] = {
      val eqb = t._1
      val filter = t._2
      val ice = new SparkIntervalConditionExtractor(eqb)
      filter match {
        case ice(ic) => Some(RangeFilterSpec(ic))
        case Match(AttributeReference(nm, dt, _, _), Literal(value, _))
          if value .isInstanceOf[UTF8String] =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield MatchFilterSpec(ec, ec.column, value.toString)
        case Match(AttributeReference(nm, dt, _, _), Literal(value, _)) =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield MatchFilterSpec(ec, ec.column, value)
        case MatchPhrase(AttributeReference(nm, dt, _, _), Literal(value, _))
          if value .isInstanceOf[UTF8String] =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield MatchPhraseFilterSpec(ec, ec.column, value.toString)
        case MatchPhrase(AttributeReference(nm, dt, _, _), Literal(value, _)) =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield MatchPhraseFilterSpec(ec, ec.column, value)
        case Like(AttributeReference(nm, dt, _, _), Literal(value, _))
          if value.isInstanceOf[UTF8String] =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield WildcardFilterSpec(ec, ec.column, value.toString)
        case StartsWith(AttributeReference(nm, dt, _, _), Literal(value, _))
          if value.isInstanceOf[UTF8String] =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield WildcardFilterSpec(ec, ec.column, s"${value.toString}*")
        case EndsWith(AttributeReference(nm, dt, _, _), Literal(value, _))
          if value.isInstanceOf[UTF8String] =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield WildcardFilterSpec(ec, ec.column, s"*${value.toString}")
        case Contains(AttributeReference(nm, dt, _, _), Literal(value, _))
          if value.isInstanceOf[UTF8String] =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield WildcardFilterSpec(ec, ec.column, s"*${value.toString}*")
        case RLike(AttributeReference(nm, dt, _, _), Literal(value, _))
          if value.isInstanceOf[UTF8String] =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield RegexpFilterSpec(ec, ec.column, value.toString)
        case EqualTo(AttributeReference(nm, dt, _, _), Literal(value: UTF8String, _)) =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield TermFilterSpec(ec, ec.column, value.toString)
        case EqualTo(AttributeReference(nm, dt, _, _), Literal(value, _)) =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield TermFilterSpec(ec, ec.column, value);

        case EqualTo(GetStructField(child, _, Some(nm)), Literal(value: UTF8String, _)) =>
          val (rootAttr, pathItems) = findRootAttribute(child, Nil)
          for (ec <- eqb.elasticCoumn(rootAttr.name); c <- ec.elasticColumn
            if c.property.isInstanceOf[NestedProperty] &&
              ElasticDataType.sparkDataType(ec.dataType,
                c.property.asInstanceOf[NestedProperty]) == rootAttr.dataType)
            yield NestedFilterSpec(pathItems.mkString("."),
              TermFilterSpec(findIndexProperty(eqb, pathItems)
                .getOrElse(throw new ElasticIndexException("WTF?")),
                (pathItems :+ nm).mkString("."), value.toString))

        case EqualTo(Literal(value, _), AttributeReference(nm, dt, _, _)) =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield TermFilterSpec(ec, ec.column, value);
        case EqualTo(AttributeReference(nm1, _, _, _), AttributeReference(nm2, _, _, _)) =>
          for (ec1 <- eqb.elasticCoumn(nm1);
               ec2 <- eqb.elasticCoumn(nm2))
            yield ColumnComparisonFilterSpec(ec1, ec2)
        case LessThan(ar @ AttributeReference(nm, dt, _, _), Literal(value, _)) =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield RangeFilterSpec(ec, IntervalConditionType.LT, value)
        case LessThan(Literal(value, _), AttributeReference(nm, dt, _, _)) =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield RangeFilterSpec(ec, IntervalConditionType.GT, value)
        case LessThanOrEqual(AttributeReference(nm, dt, _, _), Literal(value, _)) =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield RangeFilterSpec(ec, IntervalConditionType.LTE, value)
        case LessThanOrEqual(Literal(value, _), AttributeReference(nm, dt, _, _)) =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield RangeFilterSpec(ec, IntervalConditionType.GTE, value)
        case GreaterThan(AttributeReference(nm, dt, _, _), Literal(value, _)) =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield RangeFilterSpec(ec, IntervalConditionType.GT, value)
        case GreaterThan(Literal(value, _), AttributeReference(nm, dt, _, _)) =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield RangeFilterSpec(ec, IntervalConditionType.LT, value)
        case GreaterThanOrEqual(AttributeReference(nm, dt, _, _), Literal(value, _)) =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield RangeFilterSpec(ec, IntervalConditionType.GTE, value)
        case GreaterThanOrEqual(Literal(value, _), AttributeReference(nm, dt, _, _)) =>
          for (ec <- eqb.elasticCoumn(nm)
               if ElasticDataType.sparkDataType(ec.dataType) == dt)
            yield RangeFilterSpec(ec, IntervalConditionType.LTE, value)
        case _ => None

      }
    }
  }

  val elasticRelationTransform: ElasticTransform = {
    case (_, PhysicalOperation(projectList, filters,
    l @ LogicalRelation(d @ ElasticRelation(info, _), _, _, _))) =>
      // This is the initial ElasticQueryBuilder which all transformations
      // are based on.
      val eqb: Option[ElasticQueryBuilder] = Some(ElasticQueryBuilder(info))
      val (newFilters, eqb1) = ExprUtil.simplifyConjPred(eqb.get, filters)
      translateProjectFilter(Some(eqb1), projectList, newFilters)
    case _ =>
      Nil
  }
}
