package org.apache.spark.sql.catalyst.elastic.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OrderPreservingUnaryNode}

case class Offset(offsetExpr: Expression, child: LogicalPlan) extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = child.output
}

