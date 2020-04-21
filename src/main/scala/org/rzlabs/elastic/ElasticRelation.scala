package org.rzlabs.elasticsearch

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.rzlabs.elastic.{ElasticDataType, ElasticRDD, QuerySpec, Utils}
import org.rzlabs.elastic.client.NestedProperty
import org.rzlabs.elastic.metadata.ElasticRelationInfo

case class ElasticAttribute(exprId: ExprId, name: String, dataType: DataType, tf: String = null)

/**
 *
 * @param qrySpec
 * @param outputAttrSpec Attributes to be output from the RawDataSourceScanExec. Each output attribute is
 *                       based on an Attribute in the original logical plan. The association is based on
 *                       the exprId of `NamedExpression`.
 */
case class ElasticQuery(qrySpec: QuerySpec,
                        outputAttrSpec: Option[List[ElasticAttribute]]) {

  private def schemaFromQuerySpec(info: ElasticRelationInfo) = {
    qrySpec.schemaFromQuerySpec(info)
  }

  private lazy val schemaFromOutputSpec: StructType = {
    StructType(outputAttrSpec.getOrElse(Nil).map {
      case ElasticAttribute(_, name, dataType, _) =>
        new StructField(name, dataType)
    })
  }

  def schema(info: ElasticRelationInfo): StructType = {
    schemaFromOutputSpec.length match {
      case 0 => schemaFromQuerySpec(info)
      case _ => schemaFromOutputSpec
    }
  }

  private def outputAttrsFromQuerySpec(info: ElasticRelationInfo): Seq[Attribute] = {
    schemaFromQuerySpec(info).map {
      case StructField(name, dataType, _, _) => AttributeReference(name, dataType)()
    }
  }

  private lazy val outputAttrsFromOutputSpec: Seq[Attribute] = {
    outputAttrSpec.getOrElse(Nil).map {
      case ElasticAttribute(exprId, name, dataType, _) =>
        AttributeReference(name, dataType)(exprId)
    }
  }

  def outputAttrs(info: ElasticRelationInfo): Seq[Attribute] = {
    outputAttrsFromOutputSpec.size match {
      case 0 => outputAttrsFromQuerySpec(info)
      case _ => outputAttrsFromOutputSpec
    }
  }

  def getValTFMap(): Map[String, String] = {
    outputAttrSpec.getOrElse(Nil).map {
      case ElasticAttribute(_, name, _, tf) =>
        name -> tf
    }.toMap
  }
}

case class ElasticRelation(val info: ElasticRelationInfo,
                           val elasticQuery: Option[ElasticQuery])
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

  def buildInternalScan: RDD[InternalRow] = {
    elasticQuery.map(new ElasticRDD(sqlContext, info, _)).getOrElse(null)
  }

  override def buildScan(): RDD[Row] = {
    buildInternalScan.asInstanceOf[RDD[Row]]
  }

  override def toString() = {
    elasticQuery.map { eq =>
      s"ElasticQuery: ${Utils.toPrettyJson(scala.util.Left(eq))}"
    }.getOrElse {
      info.toString
    }
  }
}
