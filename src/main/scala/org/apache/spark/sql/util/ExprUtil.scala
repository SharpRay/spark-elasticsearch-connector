package org.apache.spark.sql.util

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.types._
import org.rzlabs.elastic.ElasticQueryBuilder

object ExprUtil {

  def simplifyConjPred(eqb: ElasticQueryBuilder, filters: Seq[Expression]): (
    Seq[Expression], ElasticQueryBuilder) = {

    var newFilters = Seq[Expression]()
    filters.foreach {filter =>
      for (nf <- simplifyPred(eqb, filter)) {
        newFilters = newFilters :+ nf
      }
    }
    (newFilters, eqb)
  }

  def simplifyPred(eqb: ElasticQueryBuilder, filter: Expression): Option[Expression] = filter match {
    case And(le, re) => simplifyBinaryPred(eqb, le, re, true)
    case Or(le, re) => simplifyBinaryPred(eqb, le, re, false)
    case SimplifyCast(e) => simplifyPred(eqb, e)
    case e => e match {
      case SimplifyNotNullFilter(se) =>
        /**
         * nullFilter may equals Concat(a, "abc") after simplify IsNotNull(Concat(a, "abc"))
         * This is also null preserving because either null child of Concat will
         * lead to null result.
         *
         * The null preserving expression may includes:
         *
         *  1. Cast if its child is null preserving;
         *  2. BinaryArithmetic if this children is null preserving;
         *  3. UnaryMinus if its child is null preserving;
         *  4. UnaryPositive if its child is null preserving;
         *  5. Abs if its child is null preserving;
         *  6. Concat if its child is null preserving;
         *  ...
         */
        if (se.nullable) {
          if (nullPreserving(se)) {
            // e.g., Concat(a, "123") will generate IsNotNull(a) here, and Concat(a, "123")
            // will translate to ScriptExtractionFunctionSpec.
            // Concat(a, b) will generate And(IsNotNull(a), IsNotNull(b)) here, and
            // Concat(a, b) will push down to Elasticsearch.
            val nullableAttrRefs = nullableAttributes(eqb, se.references)
            nullableAttrRefs.foldLeft(Option.empty[Expression]) {
              (le, ar) => if (le.isEmpty) {
                Some(IsNotNull(ar))
              } else {
                Some(And(le.get, IsNotNull(ar)))
              }
            }
          } else Some(se) // no isNotNull predicates generated.
        } else None // Literal(true) because it's not nullable

      case fe @ IsNull(ce) =>
        if (ce.nullable) {
          if (nullPreserving(ce)) {
            val nullableAttrRefs = nullableAttributes(eqb, ce.references)
            if (nullableAttrRefs.isEmpty) {
              Some(alwaysFalseExpr)
            } else Some(fe)
          } else Some(alwaysFalseExpr)  // not null preserving expr means any input won't result null.
        } else Some(alwaysFalseExpr)  // IsNull(not nullable expr) always false

      case _ => Some(e)
    }
  }

  def simplifyBinaryPred(eqb: ElasticQueryBuilder, le: Expression, re: Expression,
                         conj: Boolean): Option[Expression] = {

    val newLe = simplifyPred(eqb, le)
    val newRe = simplifyPred(eqb, re)
    val newFilter = if (newLe.nonEmpty) {
      if (newRe.nonEmpty) {
        if (conj) {
          Some(And(newLe.get, newRe.get))
        } else {
          Some(Or(newLe.get, newRe.get))
        }
      } else newLe
    } else {
      if (newRe.nonEmpty) {
        newRe
      } else None
    }
    newFilter
  }

  /**
   * Simplify Case expression by removing inner most cast of redundant
   * @param oe
   * @param odt
   * @return
   */
  def simplifyCast(oe: Expression, odt: DataType): Expression = oe match {
    case Cast(ie, idt, _) if odt.isInstanceOf[NumericType] && (idt.isInstanceOf[DoubleType] ||
      idt.isInstanceOf[FloatType] || idt.isInstanceOf[DecimalType]) => Cast(ie, odt)
    case _ => oe
  }

  def and(exprs: Seq[Expression]): Option[Expression] = exprs.size match {
    case 0 => None
    case 1 => exprs.headOption
    case _ => Some(exprs.foldLeft[Expression](null) { (le, e) =>
      if (le == null) e else And(le, e)
    })
  }

  /**
   * This is different from transformDown because if rule transforms an Expression,
   * we don't try to appy any more transformations.
   * @param e
   * @param rule
   * @return
   */
  def transformReplace(e: Expression,
                       rule: PartialFunction[Expression, Expression]): Expression = {
    val afterRule = CurrentOrigin.withOrigin(e.origin) {
      rule.applyOrElse(e, identity[Expression])
    }
    if (e.fastEquals(afterRule)) {
      e.transformDown(rule)
    } else {
      afterRule
    }
  }

  private[this] object SimplifyCast {
    def unapply(e: Expression): Option[Expression] = e match {
      case Cast(ie @ Cast(_, _, _), dt, _) =>
        val c = simplifyCast(ie, dt)
        if (c == ie) None else Some(ie)
      case _ => None
    }
  }

  private[this] object SimplifyNotNullFilter {
    private[this] val trueFilter = Literal(true)

    def unapply(e: Expression): Option[Expression] = e match {
      case Not(IsNull(c)) if c.nullable => Some(IsNotNull(c))
      case IsNotNull(c) if c.nullable => Some(c)
      case Not(IsNull(c)) if !c.nullable => Some(trueFilter)
      case IsNotNull(c) if !c.nullable => Some(trueFilter)
      case _ => None
    }
  }

  private[this] def nullPreserving(e: Expression): Boolean = e match {
    case Literal(v, _) if v == null => false
    case _ if e.isInstanceOf[Expression] => true
    // TODO: Expand the case below
    case Cast(_, _, _) | BinaryArithmetic(_, _) | UnaryMinus(_) | UnaryPositive(_) | Abs(_) |
         Concat(_) => e.children.filter(_.isInstanceOf[Expression]).foldLeft(true) {
      (lb, ce) => if (nullPreserving(ce) && lb) true else false
    }
    case _ => false
  }

  private[this] def nullableAttributes(eqb: ElasticQueryBuilder,
                                       references: AttributeSet): List[AttributeReference] = {
    references.foldLeft(List[AttributeReference]()) {
      (list, reference) =>
        var arList = list
        val dc = eqb.elasticCoumn(reference.name)
        if (dc.nonEmpty) {
          dc.get match {
            case d if d.elasticColumn.nonEmpty && reference.isInstanceOf[AttributeReference] =>
              arList = arList :+ reference.asInstanceOf[AttributeReference]
            case _ => None
          }
        }
        arList
    }
  }

  private val alwaysFalseExpr = EqualTo(Literal(1), Literal(2))
}
