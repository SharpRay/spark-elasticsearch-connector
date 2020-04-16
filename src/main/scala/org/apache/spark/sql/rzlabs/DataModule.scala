package org.apache.spark.sql.rzlabs

import org.apache.spark.sql.sources.elastic.ElasticPlanner
import org.apache.spark.sql.{SQLContext, Strategy}
import org.rzlabs.elastic.metadata.ElasticOptions

trait DataModule {

  def physicalRules(sqlContext: SQLContext,, options: ElasticOptions): Seq[Strategy] = Nil
}

object ElasticBaseModule extends DataModule {

  override def physicalRules(sqlContext: SQLContext, options: ElasticOptions): Seq[Strategy] = {
    val elasticPlanner = ElasticPlanner(sqlContext, options)
    Seq(new ElasticStrategy(elasticPlanner))
  }
}
