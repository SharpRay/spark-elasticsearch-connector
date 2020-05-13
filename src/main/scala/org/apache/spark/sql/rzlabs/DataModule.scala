package org.apache.spark.sql.rzlabs

import org.apache.spark.sql.catalyst.elastic.parser.ElasticCatalystSqlParser
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.elastic.{ElasticPlanner, ElasticStrategy}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, Strategy}

//trait DataModule {
//
//  def physicalRules(sqlContext: SQLContext, options: ElasticOptions): Seq[Strategy] = Nil
//}
//
//object ElasticBaseModule extends DataModule {
//
//  override def physicalRules(sqlContext: SQLContext, options: ElasticOptions): Seq[Strategy] = {
//    val elasticPlanner = ElasticPlanner(sqlContext, options)
//    Seq(new ElasticStrategy(elasticPlanner))
//  }
//}

class ElasticExtensionsBuilder extends (SparkSessionExtensions => Unit) {

  import ElasticExtensionsBuilder._

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser(parserBuilder)
    extensions.injectPlannerStrategy(strategyBuilder)
  }
}

object ElasticExtensionsBuilder {

  type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
  type StrategyBuilder = SparkSession => Strategy

  val parserBuilder: ParserBuilder = (sparkSession, _) => {
    val sqlConf = new SQLConf
    val sparkConf = sparkSession.sparkContext.getConf
    sparkConf.getAll.foreach { case (k, v) =>
      sqlConf.setConfString(k, v)
    }
    new ElasticCatalystSqlParser(sqlConf)
  }

  val strategyBuilder: StrategyBuilder = sparkSession => {
    val elasticPlanner = ElasticPlanner(sparkSession.sqlContext)
    new ElasticStrategy(elasticPlanner)
  }
}
