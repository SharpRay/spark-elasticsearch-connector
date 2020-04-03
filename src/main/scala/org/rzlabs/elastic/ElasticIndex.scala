package org.rzlabs.elastic

import org.apache.spark.sql.types._
import org.rzlabs.elastic.client._

case class ElasticIndex(name: String,
                        `type`: String,
                        columns: Map[String, ElasticColumn])

case class ElasticColumn(name: String,
                         property: IndexProperty,
                         dataType: ElasticDataType.Value,
                         keywordFields: Seq[String] = null)

object ElasticColumn {

  def apply(name: String, property: IndexProperty): ElasticColumn = property.dataType match {
    case ElasticDataType.Text => ElasticColumn(name, property, ElasticDataType.Text,
        property.asInstanceOf[TextProperty].keywordFields())
    case t @ (ElasticDataType.Keyword |
      ElasticDataType.Date |
      ElasticDataType.Long |
      ElasticDataType.Int |
      ElasticDataType.Short |
      ElasticDataType.Byte |
      ElasticDataType.Float |
      ElasticDataType.Double |
      ElasticDataType.Boolean |
      ElasticDataType.Nested) => ElasticColumn(name, property, t)
    case ElasticDataType.Unknown => ElasticColumn(name, property)
  }
}

object ElasticDataType extends Enumeration {
  val Unknown = Value("UNKNOWN")
  val Text = Value("TEXT")
  val Keyword = Value("KEYWORD")
  val Long = Value("LONG")
  val Int = Value("INT")
  val Short = Value("SHORT")
  val Byte = Value("BYTE")
  val Float = Value("FLOAT")
  val Double = Value("DOUBLE")
  val Date = Value("DATE")
  val Boolean = Value("BOOLEAN")
  val Nested = Value("NESTED")

//  def sparkDataType(t: String, property: NestedProperty = null): DataType =
//    sparkDataType(ElasticDataType.withName(t), property)

  def sparkDataType(t: ElasticDataType.Value, property: NestedProperty = null): DataType = t match {
    case Text | Keyword => StringType
    case Long => LongType
    case Int => IntegerType
    case Short => ShortType
    case Byte => ByteType
    case Float => FloatType
    case Double => DoubleType
    case Date => DateType
    case Boolean => BooleanType
    case Nested => sparkStructType(property)
  }

  private def sparkStructType(property: NestedProperty): StructType = {
    StructType(property.properties.map(p => {
      val fieldName = p._1
      val fieldType = p._2.dataType match {
        case ElasticDataType.Text | ElasticDataType.Keyword => StringType
        case ElasticDataType.Date => DateType
        case ElasticDataType.Long => LongType
        case ElasticDataType.Int => IntegerType
        case ElasticDataType.Short => ShortType
        case ElasticDataType.Byte => ByteType
        case ElasticDataType.Float => FloatType
        case ElasticDataType.Double => DoubleType
        case ElasticDataType.Boolean => BooleanType
        case ElasticDataType.Nested => sparkStructType(p._2.asInstanceOf[NestedProperty])
      }
      StructField(fieldName, fieldType)
    }).toSeq)
  }
}