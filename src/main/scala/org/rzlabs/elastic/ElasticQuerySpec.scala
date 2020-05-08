package org.rzlabs.elastic

import java.io.{ByteArrayOutputStream, InputStream}

import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.sources.elastic.{CloseableIterator, ElasticSearchResultIterator}
import org.apache.spark.sql.types.{StructField, StructType}
import org.rzlabs.elastic.client.{ElasticClient, ResultRow}
import org.rzlabs.elastic.metadata.{ElasticRelationColumn, ElasticRelationInfo}

object Order extends Enumeration {
  val ASC = Value("ASC")
  val DESC = Value("DESC")
}

object OrderName extends Enumeration {
  val order = Value("ORDER")
}

case class LimitSpec(size: Int)

case class OffsetSpec(from: Int)

case class SortSpec(sort: Map[String, Map[OrderName.Value, Order.Value]])

sealed trait QuerySpec extends Product {

  val index: String
  val `type`: Option[String]

  def apply(is: InputStream,
            conn: ElasticClient,
            onDone: => Unit = (),
            fromList: Boolean = false): CloseableIterator[ResultRow]

  def schemaFromQuerySpec(info: ElasticRelationInfo): StructType

  def executeQuery(client: ElasticClient): CloseableIterator[ResultRow] = {
    client.executeQueryAsStream(this)
  }

  def mapSparkColNameToElasticColName(info: ElasticRelationInfo): Map[String, String] = Map()
}

@JsonIgnoreProperties(Array("index", "type"))
case class SearchQuerySpec(index: String,
                           `type`: Option[String],
                           @JsonProperty("_source") columns: List[String],
                           @JsonProperty("query") filter: Option[FilterSpec],
                           @JsonProperty("from") from: Option[Int],
                           @JsonProperty("size") size: Option[Int]) extends QuerySpec {

  def toJSON(): String = {
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    val data = this.copy()
    val stream = new ByteArrayOutputStream()
    objectMapper.writeValue(stream, data)
    stream.toString()
  }

  override def schemaFromQuerySpec(info: ElasticRelationInfo): StructType = {
    val fields = columns.map { col =>
      StructField(col, ElasticDataType.sparkDataType(info.indexInfo.columns(col).dataType))
    }

    StructType(fields)
  }

  override def apply(is: InputStream,
                     conn: ElasticClient,
                     onDone: => Unit = (),
                     fromList: Boolean = false): CloseableIterator[ResultRow] = {
    ElasticSearchResultIterator(is, onDone, fromList)
  }
}

sealed trait FilterSpec

/**
 * e.g.,
 * """
 *   {
 *     "range": {
 *       "publish_time": {
 *         "gte": "2020-01-01T00:00:00",
 *         "lt": "2021-01-01T00:00:00"
 *       }
 *     }
 *   }
 * """
 * @param range
 */
case class RangeFilterSpec(range: Map[String, Map[String, Any]]) extends FilterSpec

object RangeFilterSpec {

  def apply(ic: IntervalCondition): RangeFilterSpec = {
    val columnName = ic.dtGrp.elasticColumn.column
    val millis = ic.dt.getMillis
    var range = Map[String, Map[String, Any]]()
    ic.`type` match {
      case IntervalConditionType.EQ =>
        range = range + (columnName ->
          Map(IntervalConditionType.GTE.toString -> millis,
            IntervalConditionType.LTE.toString -> millis))
      case _ => range = range + (columnName -> Map(ic.`type`.toString -> millis))
    }
    new RangeFilterSpec(range)
  }

  def apply(ec: ElasticRelationColumn,
            conditionType: IntervalConditionType.Value,
            value: Any) = conditionType match {
    case IntervalConditionType.EQ =>
      new RangeFilterSpec(Map[String, Map[String, Any]](ec.column ->
        Map[String, Any](IntervalConditionType.GTE.toString -> value,
          IntervalConditionType.LTE.toString -> value)))
    case _ =>
      new RangeFilterSpec(Map[String, Map[String, Any]](ec.column ->
        Map[String, Any](conditionType.toString -> value)))
  }
}

case class MatchFilterSpec(`match`: Map[String, Any]) extends FilterSpec

object MatchFilterSpec {

  def apply(ec: ElasticRelationColumn, name: String, value: Any) = ec.dataType match {
    case ElasticDataType.Text =>
      new MatchFilterSpec(
        Map[String, Any](name -> value)
      )
    case _ =>
      throw new ElasticIndexException("Only text field could use match predicate.")
  }
}

case class MatchPhraseFilterSpec(match_phrase: Map[String, Any]) extends FilterSpec

object MatchPhraseFilterSpec {

  def apply(ec: ElasticRelationColumn, name: String, value: Any) = ec.dataType match {
    case ElasticDataType.Text =>
      new MatchPhraseFilterSpec(
        Map[String, Any](name -> value)
      )
    case _ =>
      throw new ElasticIndexException("Only text field could use match_phrase predicate.")
  }
}

case class TermFilterSpec(term: Map[String, Any]) extends FilterSpec

object TermFilterSpec {

  def apply(ec: ElasticRelationColumn, name: String, value: Any) = ec.dataType match {
    case ElasticDataType.Text if ec.keywordField.isDefined =>
      new TermFilterSpec(
        Map[String, Any](s"$name.${ec.keywordField.get}" -> value))
    case ElasticDataType.Text =>
      throw new ElasticIndexException(
        "Text type column without keyword field cannot be applied EqualTo expression.")
    case ElasticDataType.Unknown =>
      throw new ElasticIndexException(s"'$name' column is type-unknown.")
    case _ =>
      new TermFilterSpec(
        Map[String, Any](name -> value)
      )
  }
}

case class TermsFilterSpec(terms: Map[String, List[Any]]) extends FilterSpec {
  def this(name: String, vals: List[Any]) {
    this(Map[String, List[Any]](name -> vals))
  }
}

case class FieldSpec(field: String)

case class ExistsFilterSpec(exists: FieldSpec) extends FilterSpec {
  def this(name: String) {
    this(FieldSpec(name))
  }
}

case class InlineSpec(inline: String, lang: String) {
  def this(inline: String) {
    this(inline, "painless")
  }
}

case class InlineScriptSpec(script: InlineSpec)

case class ColumnComparisonFilterSpec(script: InlineScriptSpec) extends FilterSpec

object ColumnComparisonFilterSpec {

  def apply(ec1: ElasticRelationColumn, ec2: ElasticRelationColumn) = List(ec1, ec2) match {
    case list if list.exists(ec => ec.dataType == ElasticDataType.Text && ec.keywordField.isEmpty) =>
      throw new ElasticIndexException("Fielddata is disabled on text fields by default. ")
    case list if list.exists(_.dataType == ElasticDataType.Unknown) =>
      throw new ElasticIndexException("Has type-unknown column.")
    case list if list.exists(_.dataType == ElasticDataType.Text) =>
      val names = list.map { ec =>
        if (ec.dataType == ElasticDataType.Text && ec.keywordField.isDefined)
          s"${ec.column}.${ec.keywordField.get}"
        else ec.column
      }
      new ColumnComparisonFilterSpec(
        InlineScriptSpec(
          new InlineSpec(
            s"doc['${names(0)}'].value == doc['${names(1)}'].value"
          )
        )
      )
    case list =>
      new ColumnComparisonFilterSpec(
        InlineScriptSpec(
          new InlineSpec(
            s"doc['${ec1.column}'].value == doc['${ec2.column}'].value"
          )
        )
      )
  }
}

//sealed trait ConjExpressionFilterSpec extends FilterSpec
//
//case class ShouldExpressionFilterSpec(should: List[FilterSpec]) extends ConjExpressionFilterSpec
//
//case class MustExpressionFilterSpec(must: List[FilterSpec]) extends ConjExpressionFilterSpec
//
//case class MustNotExpressionFilterSpec(@JsonProperty("must_no")
//                                       mustNot: List[FilterSpec]) extends ConjExpressionFilterSpec
//
//case class FilterExpressionFilterSpec(filter: List[FilterSpec]) extends ConjExpressionFilterSpec

case class ConjExpressionFilterSpec(must: List[FilterSpec] = null,
                                   @JsonProperty("must_not") mustNot: List[FilterSpec] = null,
                                   should: List[FilterSpec] = null,
                                   filter: List[FilterSpec] = null)

case class BoolExpressionFilterSpec(bool: ConjExpressionFilterSpec) extends FilterSpec
