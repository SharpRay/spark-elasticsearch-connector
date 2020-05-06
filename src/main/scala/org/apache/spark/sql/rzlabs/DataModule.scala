package org.apache.spark.sql.rzlabs

import org.apache.spark.sql.catalyst.elastic.parser.{ElasticCatalystSqlParser, SqlBaseParser}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.elastic.{ElasticPlanner, ElasticStrategy}
import org.apache.spark.sql.{SQLContext, SparkSession, SparkSessionExtensions, Strategy}
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

class ElasticExtensionsBuilder extends (SparkSessionExtensions => Unit) {

  import ElasticExtensionsBuilder._

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser(parserBuilder)
  }
}

object ElasticExtensionsBuilder {

  type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
  val parserBuilder: ParserBuilder = (sparkSession, _) => {
    val sqlConf = new SQLConf
    val sparkConf = sparkSession.sparkContext.getConf
    sparkConf.getAll.foreach { case (k, v) =>
      sqlConf.setConfString(k, v)
    }
    new ElasticCatalystSqlParser(sqlConf)
  }
}
