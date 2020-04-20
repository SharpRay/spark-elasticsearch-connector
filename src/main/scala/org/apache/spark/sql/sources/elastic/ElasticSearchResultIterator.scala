package org.apache.spark.sql.sources.elastic

import java.io.InputStream

import com.fasterxml.jackson.core.JsonToken
import org.apache.spark.util.NextIterator
import org.rzlabs.elastic.client.SearchResultRow
import org.fasterxml.jackson.databind.ObjectMapper.jsonMapper
import org.rzlabs.elastic.ElasticIndexException

private class ElasticSearchResultStreamingIterator(is: InputStream,
                                                   onDone: => Unit = ()
                                                  ) extends NextIterator[SearchResultRow]
  with CloseableIterator[SearchResultRow] {

  // In NextIterator the abstract `closeIfNeeded`
  // method declared in CloseableIterator is defined.

  private val factory = jsonMapper.getFactory
  private val parser = factory.createParser(is)
  private var token = parser.nextToken()  // START_OBJECT
  private var previousToken: JsonToken = null

  private def gotoNamedHitsJsonTokenNext = {
    token = parser.nextToken()  // FIELD_NAME
    while (token != JsonToken.FIELD_NAME || parser.currentName() != "hits") {
      if (token == JsonToken.START_OBJECT)
        jsonMapper.readValue(parser, classOf[Map[String, Any]])
      token = parser.nextToken()
    }
    token = parser.nextToken()
  }

  override protected def getNext(): SearchResultRow = {


    if (token == JsonToken.START_OBJECT && previousToken == null) {
      gotoNamedHitsJsonTokenNext
      gotoNamedHitsJsonTokenNext  // START_ARRAY
      token = parser.nextToken()  // STARRT_OBJECT or END_ARRAY (empty result set)
    }
    if (token == JsonToken.START_OBJECT) {
      val r: SearchResultRow = SearchResultRow(
        jsonMapper.readValue(parser, classOf[Map[String, Any]])("_source")
          .asInstanceOf[Map[String, Any]])
      previousToken = token
      token = parser.nextToken()
      r
    } else if (token == JsonToken.END_ARRAY) {
      finished = true
      null
    } else {
      throw new ElasticIndexException(s"Unexpected search result")
    }
  }

  override protected def close(): Unit = {
    parser.close()
    onDone
  }
}

private class ElasticSearchResultStaticIterator(is: InputStream,
                                                onDone: => Unit = ()) extends NextIterator[SearchResultRow]
  with CloseableIterator[SearchResultRow] {

  private var rowList: List[SearchResultRow] = List()

  private val factory = jsonMapper.getFactory
  private val parser = factory.createParser(is)
  private var token = parser.nextToken()

  gotoNamedHitsJsonTokenNext
  gotoNamedHitsJsonTokenNext  // START_ARRAY
  token = parser.nextToken()  // START_OBJECT or END_ARRAY (empty result set)
  if (token != JsonToken.END_ARRAY) {
    val events: List[Map[String, Any]] =
      jsonMapper.readValue(parser, classOf[List[Map[String, Any]]])
    rowList = rowList ++
      events.map(map => SearchResultRow(map("_source").asInstanceOf[Map[String, Any]]))
  }

  // Here will be END_ARRAY token
  assert(parser.nextToken() == JsonToken.END_ARRAY)

  onDone
  parser.close()

  private def gotoNamedHitsJsonTokenNext = {
    token = parser.nextToken()  // FIELD_NAME
    while (token != JsonToken.FIELD_NAME || parser.currentName() != "hits") {
      if (token == JsonToken.START_OBJECT)
        jsonMapper.readValue(parser, classOf[Map[String, Any]])
      token = parser.nextToken()
    }
    token = parser.nextToken()
  }

  val iter = rowList.toIterator

  override protected def getNext(): SearchResultRow = {
    if (iter.hasNext) {
      iter.next()
    } else {
      finished = true
      null
    }
  }

  override protected def close(): Unit = () // This because the onDone is called in constructor
}

object ElasticSearchResultIterator {

  def apply(is: InputStream,
            onDone: => Unit = (),
            fromList: Boolean = false): CloseableIterator[SearchResultRow] = {

    if (fromList) {
      new ElasticSearchResultStaticIterator(is, onDone)
    } else {
      new ElasticSearchResultStreamingIterator(is, onDone)
    }
  }
}
