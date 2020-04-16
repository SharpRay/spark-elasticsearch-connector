package org.rzlabs.elasticsearch

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.rzlabs.elastic.ElasticDataType
import org.rzlabs.elastic.client.NestedProperty
import org.rzlabs.elastic.metadata.ElasticRelationInfo

case class ElasticAttribute(exprId: ExprId, name: String, dataType: DataType, tf: String = null)

case class ElasticRelation(val info: ElasticRelationInfo)
                          (@transient val sqlContext: SQLContext)
    extends BaseRelation with TableScan{

  override val needConversion: Boolean = false

  override def schema: StructType = {
    StructType(
      info.indexInfo.columns.map(c => {
        if (c._2.dataType == ElasticDataType.Nested) {
          StructField(c._1,
            ElasticDataType.sparkDataType(ElasticDataType.Nested,
              c._2.property.asInstanceOf[NestedProperty])
          )
        } else {
          StructField(c._1, ElasticDataType.sparkDataType(c._2.dataType))
        }
      }).toSeq
    )
  }

  override def buildScan(): RDD[Row] = {
    return null
  }
}
