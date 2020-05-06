package org.rzlabs

import org.rzlabs.elastic.SearchQuerySpec
import org.scalatest.funsuite.AnyFunSuite
import spray.json._

class BaseTest extends AnyFunSuite {

  val spec = SearchQuerySpec("abc", Some("123"), List("abc"), None, None, None)

  test("convert to JSON") {
    assert(spec.toJSON().parseJson ==
      """
        |{
        |  "_source": ["abc"],
        |  "query": null
        |}
      """.stripMargin.parseJson)
  }
}