package org.rzlabs.elastic

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.MyLogging
import org.fasterxml.jackson.databind.ObjectMapper.jsonMapper

object Utils extends MyLogging {

  /**
   * transform list[Option] to Option[List]
   * @param a
   * @tparam A
   * @return
   */
  def sequence[A](a: List[Option[A]]): Option[List[A]] = a match {
    case Nil => Some(Nil)
    case head :: tail => head.flatMap(h => sequence(tail).map(h :: _))
  }

  def toPrettyJson(obj: Either[AnyRef, JsonNode]) = {
    jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(
      if (obj.isLeft) obj.left else obj.right
    )
  }
}
