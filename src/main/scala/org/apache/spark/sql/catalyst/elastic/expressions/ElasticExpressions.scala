package org.apache.spark.sql.catalyst.elastic.expressions

import org.apache.spark.sql.catalyst.expressions.{BinaryComparison, Expression, NullIntolerant, StringRegexExpression}

case class Match(left: Expression, right: Expression)
  extends BinaryComparison with NullIntolerant {

  override def symbol: String = "MATCH"
}

case class MatchPhrase(left: Expression, right: Expression)
  extends BinaryComparison with NullIntolerant {

  override def symbol: String = "MATCH_PHRASE"
}

