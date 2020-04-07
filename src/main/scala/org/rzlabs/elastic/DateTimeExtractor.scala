package org.rzlabs.elastic

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.rzlabs.elastic.metadata.ElasticRelationColumn

case class DateTimeGroupingElem(outputName: String,
                                elasticColumn: ElasticRelationColumn,
                                formatToApply: String,
                                timeZone: Option[String],
                                pushedExpression: Expression,
                                inputFormat: Option[String] = None)

object ElasticColumnExtractor {

  def unapply(e: Expression)(
    implicit eqb: ElasticQueryBuilder): Option[ElasticRelationColumn] = e match {
    case AttributeReference(nm, _, _, _) =>
      val elasticColumn = eqb.elasticCoumn(nm)
      elasticColumn.filter(_.elasticColumn.exists(_.dataType == ElasticDataType.Date))
    case _ => None
  }
}

class SparkNativeTimeElementExtractor(implicit val eqb: ElasticQueryBuilder) {

  self =>

  import SparkNativeTimeElementExtractor._

  def unapply(e: Expression): Option[DateTimeGroupingElem] = e match {
    case ElasticColumnExtractor(ec) if e.dataType == DateType =>
      Some(DateTimeGroupingElem(eqb.nextAlias, ec, DATE_FORMAT,
        Some(eqb.relationInfo.options.timeZoneId), e))
    case Cast(c @ ElasticColumnExtractor(ec), DateType, _) =>
      // e.g., "cast(time as date)"
      Some(DateTimeGroupingElem(eqb.nextAlias, ec, DATE_FORMAT,
        Some(eqb.relationInfo.options.timeZoneId), c))
    case Cast(self(dtGrp), DateType, _) =>
      // e.g., "cast(from_utc_timestamp(time, 'GMT') as date)"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        DATE_FORMAT, dtGrp.timeZone, dtGrp.pushedExpression))
    case ElasticColumnExtractor(ec) if e.dataType == StringType =>
      Some(DateTimeGroupingElem(eqb.nextAlias, ec, TIMESTAMP_FORMAT,
        Some(eqb.relationInfo.options.timeZoneId), e))
    case Cast(self(dtGrp), StringType, _) =>
      // e.g., "cast(time as string)"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        dtGrp.formatToApply, dtGrp.timeZone, dtGrp.pushedExpression))
    case ElasticColumnExtractor(ec) if e.dataType == TimestampType =>
      Some(DateTimeGroupingElem(eqb.nextAlias, ec, TIMESTAMP_FORMAT,
        Some(eqb.relationInfo.options.timeZoneId), e))
    case Cast(self(dtGrp), TimestampType, _) =>
      // e.g., "cast(to_date(time) as timestamp)"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        TIMESTAMP_FORMAT, dtGrp.timeZone, dtGrp.pushedExpression))
    case ParseToDate(self(dtGrp), fmt, _) if fmt.isInstanceOf[Option[Literal]] =>
      val fmtStr = if (fmt.nonEmpty) {
        fmt.map(_.asInstanceOf[Literal].value.toString).get
      } else DATE_FORMAT
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        fmtStr, dtGrp.timeZone, dtGrp.pushedExpression))
    case Year(self(dtGrp)) =>
      // e.g., "year(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        YEAR_FORMAT, dtGrp.timeZone, dtGrp.pushedExpression))
    case DayOfMonth(self(dtGrp)) =>
      // e.g., "dayofmonth(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        DAY_OF_MONTH_FORMAT, dtGrp.timeZone, dtGrp.pushedExpression))
    case DayOfYear(self(dtGrp)) =>
      // e.g., "dayofyear(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        DAY_OF_YEAR_FORMAT, dtGrp.timeZone, dtGrp.pushedExpression))
    case Month(self(dtGrp)) =>
      // e.g., "month(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        MONTH_FORMAT, dtGrp.timeZone, dtGrp.pushedExpression))
    case WeekOfYear(self(dtGrp)) =>
      // e.g., "weekofyear(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        WEEKOFYEAR_FORMAT, dtGrp.timeZone, dtGrp.pushedExpression))
    case Hour(self(dtGrp), _) =>
      // e.g., "hour(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        HOUR_FORMAT, dtGrp.timeZone, dtGrp.pushedExpression))
  }
}

object SparkNativeTimeElementExtractor {

  val DATE_FORMAT = "YYYY-MM-dd"
  val TIMESTAMP_FORMAT = "YYYY-MM-dd HH:mm:ss"
  val TIMESTAMP_DATEZERO_FORMAT = "YYYY-MM-dd 00:00:00"

  val YEAR_FORMAT = "YYYY"
  val MONTH_FORMAT = "MM"
  val WEEKOFYEAR_FORMAT = "ww"
  val DAY_OF_MONTH_FORMAT = "dd"
  val DAY_OF_YEAR_FORMAT = "DD"

  val HOUR_FORMAT = "HH"
  val MINUTE_FORMAT = "mm"
  val SECOND_FORMAT = "ss"
}

object IntervalConditionType extends Enumeration {
  val GT = Value
  val GTE = Value
  val LT = Value
  val LTE = Value
}

case class IntervalCondition(`type`: IntervalConditionType.Value, dt: DateTime)

class SparkIntervalConditionExtractor(eqb: ElasticQueryBuilder) {

  import SparkNativeTimeElementExtractor._

  def unapply(e: Expression): Option[ElasticQueryBuilder] = e match {
    case LessThan(timeExtractor)
  }
}
