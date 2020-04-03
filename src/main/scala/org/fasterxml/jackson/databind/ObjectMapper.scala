package org.fasterxml.jackson.databind

import com.fasterxml.jackson.databind.{DeserializationFeature, MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object ObjectMapper {

  val jsonMapper = {
    val om = new ObjectMapper()
    om.registerModule(DefaultScalaModule)
    om.registerModule(new JodaModule)
    om.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    om.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    om.configure(MapperFeature.USE_BASE_TYPE_AS_DEFAULT_IMPL, true)
    om
  }

  val smileMapper = {
    val om = new ObjectMapper(new SmileFactory())
    om.registerModule(DefaultScalaModule)
    om.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    om
  }
}
