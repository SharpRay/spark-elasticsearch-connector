package org.rzlabs.elastic

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.joda.time.{DateTime, DateTimeZone}
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
    case Minute(self(dtGrp), _) =>
      // e.g., "minute(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        MINUTE_FORMAT, dtGrp.timeZone, dtGrp.pushedExpression))
    case Second(self(dtGrp), _) =>
      // e.g., "second(cast(time as date))"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        SECOND_FORMAT, dtGrp.timeZone, dtGrp.pushedExpression))
    case UnixTimestamp(self(dtGrp), Literal(inFmt, StringType), _) =>
      // e.g., "unix_timestmap(cast(time as date), 'YYYY-MM-dd HH:mm:ss')"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        TIMESTAMP_FORMAT, dtGrp.timeZone, dtGrp.pushedExpression,
        Some(inFmt.toString)))
    case FromUnixTime(self(dtGrp), Literal(outFmt, StringType), _) =>
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        outFmt.toString, dtGrp.timeZone, dtGrp.pushedExpression))
    case FromUnixTime(c @ ElasticColumnExtractor(ec), Literal(outFmt, StringType), _) =>
      Some(DateTimeGroupingElem(eqb.nextAlias, ec, outFmt.toString,
        Some(eqb.relationInfo.options.timeZoneId), c))
    case FromUTCTimestamp(self(dtGrp), Literal(tz, StringType)) =>
      // e.g., "from_utc_timestamp(cast(time as timestamp), 'GMT')"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        TIMESTAMP_FORMAT, Some(tz.toString), dtGrp.pushedExpression))
    case FromUTCTimestamp(c @ ElasticColumnExtractor(ec), Literal(tz, StringType)) =>
      // e.g., "from_utc_timestamp(time, 'GMT')"
      Some(DateTimeGroupingElem(eqb.nextAlias, ec,
        TIMESTAMP_FORMAT, Some(tz.toString), c))
    case ToUTCTimestamp(self(dtGrp), _) =>
      // e.g., "to_utc_timestamp(cast(time as timestamp), 'GMT')"
      Some(DateTimeGroupingElem(dtGrp.outputName, dtGrp.elasticColumn,
        TIMESTAMP_FORMAT, None, dtGrp.pushedExpression))
    case ToUTCTimestamp(c @ ElasticColumnExtractor(ec), _) =>
      // e.g., "to_utc_timestamp(time, 'GMT')"
      Some(DateTimeGroupingElem(eqb.nextAlias, ec,
        TIMESTAMP_FORMAT, None, c))
    case _ => None
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
  val GT = Value("gt")
  val GTE = Value("gte")
  val LT = Value("lt")
  val LTE = Value("lte")
  val EQ = Value("eq")
}

case class IntervalCondition(`type`: IntervalConditionType.Value,
                             dt: DateTime,
                             dtGrp: DateTimeGroupingElem)

class SparkIntervalConditionExtractor(eqb: ElasticQueryBuilder) {

  import SparkNativeTimeElementExtractor._

  val timeExtractor = new SparkNativeTimeElementExtractor()(eqb)

  private def literalToDateTime(value: Any, dataType: DataType): DateTime = dataType match {
    case TimestampType =>
      // Timestamp Literal's value accurate to micro second
      new DateTime(value.toString.toLong / 1000,
        DateTimeZone.forID(eqb.relationInfo.options.timeZoneId))
    case DateType =>
      new DateTime(DateTimeUtils.toJavaDate(value.toString.toInt),
        DateTimeZone.forID(eqb.relationInfo.options.timeZoneId))
    case StringType =>
      new DateTime(value.toString,
        DateTimeZone.forID(eqb.relationInfo.options.timeZoneId))
  }

  private object DateTimeLiteralType {
    def unapply(dt: DataType): Option[DataType] = dt match {
      case StringType | DateType | TimestampType => Some(dt)
      case _ => None
    }
  }

  def unapply(e: Expression): Option[IntervalCondition] = e match {
    case LessThan(timeExtractor(dtGrp), Literal(value, DateTimeLiteralType(dt)))
      if dtGrp.formatToApply == TIMESTAMP_FORMAT ||
        dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT =>
      Some(IntervalCondition(IntervalConditionType.LT, literalToDateTime(value, dt), dtGrp))
    case LessThan(Literal(value, DateTimeLiteralType(dt)), timeExtractor(dtGrp))
      if dtGrp.formatToApply == TIMESTAMP_FORMAT ||
        dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT =>
      Some(IntervalCondition(IntervalConditionType.GT, literalToDateTime(value, dt), dtGrp))
    case LessThanOrEqual(timeExtractor(dtGrp), Literal(value, DateTimeLiteralType(dt)))
      if dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT =>
      Some(IntervalCondition(IntervalConditionType.LTE, literalToDateTime(value, dt), dtGrp))
    case LessThanOrEqual(Literal(value, DateTimeLiteralType(dt)), timeExtractor(dtGrp))
      if dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT =>
      Some(IntervalCondition(IntervalConditionType.GTE, literalToDateTime(value, dt), dtGrp))
    case GreaterThan(timeExtractor(dtGrp), Literal(value, DateTimeLiteralType(dt)))
      if dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT =>
      Some(IntervalCondition(IntervalConditionType.GT, literalToDateTime(value, dt), dtGrp))
    case GreaterThan(Literal(value, DateTimeLiteralType(dt)), timeExtractor(dtGrp))
      if dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT =>
      Some(IntervalCondition(IntervalConditionType.LT, literalToDateTime(value, dt), dtGrp))
    case GreaterThanOrEqual(timeExtractor(dtGrp), Literal(value, DateTimeLiteralType(dt)))
      if dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT =>
      Some(IntervalCondition(IntervalConditionType.GTE, literalToDateTime(value, dt), dtGrp))
    case GreaterThanOrEqual(Literal(value, DateTimeLiteralType(dt)), timeExtractor(dtGrp))
      if dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT =>
      Some(IntervalCondition(IntervalConditionType.LTE, literalToDateTime(value, dt), dtGrp))
    case EqualTo(timeExtractor(dtGrp), Literal(value, DateTimeLiteralType(dt)))
      if dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT =>
      Some(IntervalCondition(IntervalConditionType.EQ, literalToDateTime(value, dt), dtGrp))
    case EqualTo(Literal(value, DateTimeLiteralType(dt)), timeExtractor(dtGrp))
      if dtGrp.formatToApply == TIMESTAMP_FORMAT ||
        dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT =>
      Some(IntervalCondition(IntervalConditionType.EQ, literalToDateTime(value, dt), dtGrp))
    case _ => None

  }
}
