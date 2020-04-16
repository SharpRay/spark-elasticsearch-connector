package org.rzlabs.elastic

import com.fasterxml.jackson.core.Base64Variants
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLTimestamp
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.joda.time.{DateTime, DateTimeZone}

object ElasticValTransform {

  private[this] val toTSWithTZAdj = (elasticVal: Any, tz: String) => {
    val evLong = if (elasticVal.isInstanceOf[Double]) {
      elasticVal.asInstanceOf[Double].toLong
    } else if (elasticVal.isInstanceOf[BigInt]) {
      elasticVal.asInstanceOf[BigInt].toLong
    } else if (elasticVal.isInstanceOf[String]) {
      elasticVal.asInstanceOf[String].toLong
    } else if (elasticVal.isInstanceOf[Int]) {
      elasticVal.asInstanceOf[Int].toLong
    } else if (elasticVal.isInstanceOf[Integer]) {
      elasticVal.asInstanceOf[Integer].toLong
    } else {
      elasticVal
    }

    // microsecond
    new DateTime(evLong, DateTimeZone.forID(tz)).getMillis * 1000.asInstanceOf[SQLTimestamp]
  }

  private[this] val toTS = (elasticVal: Any, tz: String) => {
    if (elasticVal.isInstanceOf[Double]) {
      elasticVal.asInstanceOf[Double].toLong
    } else if (elasticVal.isInstanceOf[BigInt]) {
      elasticVal.asInstanceOf[BigInt].toLong
    } else if (elasticVal.isInstanceOf[Int]) {
      elasticVal.asInstanceOf[Int].toLong
    } else if (elasticVal.isInstanceOf[Integer]) {
      elasticVal.asInstanceOf[Integer].toLong
    } else elasticVal
  }

  private[this] val toString = (elasticVal: Any, tz: String) => {
    UTF8String.fromString(elasticVal.toString)
  }

  private[this] val toInt = (elasticVal: Any, tz: String) => {
    if (elasticVal.isInstanceOf[Double]) {
      elasticVal.asInstanceOf[Double].toInt
    } else if (elasticVal.isInstanceOf[BigInt]) {
      elasticVal.asInstanceOf[BigInt].toInt
    } else if (elasticVal.isInstanceOf[String]) {
      elasticVal.asInstanceOf[String].toInt
    } else if (elasticVal.isInstanceOf[Integer]) {
      elasticVal.asInstanceOf[Integer].toInt
    } else elasticVal
  }

  private[this] val toLong = (elasticVal: Any, tz: String) => {
    if (elasticVal.isInstanceOf[Double]) {
      elasticVal.asInstanceOf[Double].toLong
    } else if (elasticVal.isInstanceOf[BigInt]) {
      elasticVal.asInstanceOf[BigInt].toLong
    } else if (elasticVal.isInstanceOf[String]) {
      elasticVal.asInstanceOf[String].toLong
    } else if (elasticVal.isInstanceOf[Int]) {
      elasticVal.asInstanceOf[Int].toLong
    } else if (elasticVal.isInstanceOf[Integer]) {
      elasticVal.asInstanceOf[Integer].toLong
    } else elasticVal
  }

  private[this] val toFloat = (elasticVal: Any, tz: String) => {
    if (elasticVal.isInstanceOf[Double]) {
      elasticVal.asInstanceOf[Double].toFloat
    } else if (elasticVal.isInstanceOf[BigInt]) {
      elasticVal.asInstanceOf[BigInt].toFloat
    } else if (elasticVal.isInstanceOf[String]) {
      elasticVal.asInstanceOf[String].toFloat
    } else if (elasticVal.isInstanceOf[Int]) {
      elasticVal.asInstanceOf[Int].toFloat
    } else if (elasticVal.isInstanceOf[Integer]) {
      elasticVal.asInstanceOf[Integer].toFloat
    } else elasticVal
  }

  private[this] val tfMap: Map[String, (Any, String) => Any] = {
    Map[String, (Any, String) => Any](
      "toTSWithTZAdj" -> toTSWithTZAdj,
      "toTS" -> toTS,
      "toString" -> toString,
      "toInt" -> toInt,
      "toLong" -> toLong,
      "toFloat" -> toFloat
    )
  }

  def defaultValueConversion(f: StructField, elasticVal: Any): Any = f.dataType match {
    case TimestampType if elasticVal.isInstanceOf[Double] =>
      elasticVal.asInstanceOf[Double].longValue()
    case StringType if elasticVal != null => UTF8String.fromString(elasticVal.toString)
    case LongType if elasticVal.isInstanceOf[BigInt] =>
      elasticVal.asInstanceOf[BigInt].longValue()
    case LongType if elasticVal.isInstanceOf[Integer] =>
      elasticVal.asInstanceOf[Integer].longValue()
    case BinaryType if elasticVal.isInstanceOf[String] =>
      Base64Variants.getDefaultVariant.decode(elasticVal.asInstanceOf[String])
    case _ => elasticVal

  }

  def sparkValue(f: StructField, elasticVal: Any, tfName: Option[String], tz: String): Any = {
    tfName match {
      case Some(tf) if (tfMap.contains(tf) && elasticVal != null) => tfMap(tf)(elasticVal, tz)
      case _ => defaultValueConversion(f, elasticVal)
    }
  }

  def getTFName(sparkDT: DataType, adjForTZ: Boolean = false): String = sparkDT match {
    case TimestampType if adjForTZ => "toTSWithTZAdj"
    case TimestampType => "toTS"
    case StringType if !adjForTZ => "toString"
    case ShortType if !adjForTZ => "toInt"
    case LongType => "toLong"
    case FloatType => "toFloat"
    case _ => ""
  }
}
