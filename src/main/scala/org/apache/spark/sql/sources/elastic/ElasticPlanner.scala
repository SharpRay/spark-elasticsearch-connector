package org.apache.spark.sql.sources.elastic

import org.apache.spark.sql.SQLContext
import org.rzlabs.elastic.metadata.ElasticOptions

class ElasticPlanner(val sqlContext: SQLContext, val options: ElasticOptions)
  extends ElasticTransforms with ProjectFilterTransform {

}
