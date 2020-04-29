package org.apache.spark.sql.rzlabs

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.sources.elastic.{ElasticPlanner, ElasticStrategy}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{MyLogging, SQLContext, SparkSession, SparkSessionExtensions, Strategy}
import org.rzlabs.elastic.ElasticIndexException
import org.rzlabs.elastic.metadata.ElasticOptions

trait DataModule {

  def physicalRules(sqlContext: SQLContext, options: ElasticOptions): Seq[Strategy] = Nil
}

object ElasticBaseModule extends DataModule {

  override def physicalRules(sqlContext: SQLContext, options: ElasticOptions): Seq[Strategy] = {
    val elasticPlanner = ElasticPlanner(sqlContext, options)
    Seq(new ElasticStrategy(elasticPlanner))
  }
}

class ElasticParser(parser: ParserInterface) extends ParserInterface with MyLogging {

  override def parsePlan(sqlText: String): LogicalPlan = {
    println("sqlText ============= " + sqlText)
    parser.parsePlan(sqlText)
  }

  override def parseExpression(sqlText: String): Expression =
    parser.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    parser.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    parser.parseFunctionIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    parser.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    parser.parseDataType(sqlText)
}

class ElasticExtensionsBuilder extends (SparkSessionExtensions => Unit) {

  import ElasticExtensionsBuilder._

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser(parserBuilder)
  }
}

object ElasticExtensionsBuilder {

  type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
  val parserBuilder: ParserBuilder = (_, parser) => new ElasticParser(parser)
}
