package org.apache.spark.sql.sources.elastic

import java.io.InputStream

import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.NextIterator
import org.rzlabs.elastic.client.{IndexProperty, NestedProperty, ResultRow, SearchResultRow}
import org.fasterxml.jackson.databind.ObjectMapper.jsonMapper
import org.rzlabs.elastic.ElasticIndexException
import org.rzlabs.elastic.metadata.ElasticRelationInfo

import scala.reflect.{ClassTag, classTag}

private class ElasticSearchResultStreamingIterator(relationInfo: ElasticRelationInfo,
                                                   is: InputStream,
                                                   onDone: => Unit = ()
                                                  ) extends ElasticSearchResultIterator(relationInfo)
  with CloseableIterator[SearchResultRow] {

  // In NextIterator the abstract `closeIfNeeded`
  // method declared in CloseableIterator is defined.
  private val factory = jsonMapper.getFactory
  private val parser = factory.createParser(is)
  private var token = parser.nextToken()  // START_OBJECT
  private var previousToken: JsonToken = null

  private def gotoNamedHitsJsonTokenNext = {
    token = parser.nextToken()  // FIELD_NAME
    while (token != JsonToken.FIELD_NAME || parser.getCurrentName() != "hits") {
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
      val event: Map[String, Any] =
        jsonMapper.readValue(parser, new TypeReference[Map[String, Any]]() {})("_source")
          .asInstanceOf[Map[String, Any]].map {
        case (k, v) =>
          relationInfo.indexInfo.columns.get(k) match {
            case Some(ec) => (k, processValue(v, ec.property))
            case None => throw new ElasticIndexException("WTF?")
          }
      }
      val r: SearchResultRow = SearchResultRow(event)
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

private class ElasticSearchResultStaticIterator(relationInfo: ElasticRelationInfo, is: InputStream,
                                                onDone: => Unit = ()) extends ElasticSearchResultIterator(relationInfo)
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
    while (token != JsonToken.FIELD_NAME || parser.getCurrentName() != "hits") {
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

private abstract class ElasticSearchResultIterator(relationInfo: ElasticRelationInfo)
  extends NextIterator[SearchResultRow] {

  def processValue(v: Any, property: IndexProperty): Any = {
    v match {
      case v: Seq[_] if v.nonEmpty && v(0).isInstanceOf[Map[String, Any]] =>
        property match {
          case prop if prop.isInstanceOf[NestedProperty] =>
            val nestedFieldNum = prop.asInstanceOf[NestedProperty].properties.size
            val valArr = Array.fill[Seq[Any]](nestedFieldNum)(Seq[Any]())
            v.asInstanceOf[Seq[Map[String, Any]]] foreach { m =>
              val nestedProp = prop.asInstanceOf[NestedProperty]
              nestedProp.properties.keys.zipWithIndex.foreach {
                case (k, idx) =>
                  m.get(k) match {
                    case Some(s) =>
                      valArr(idx) = valArr(idx) :+ processValue(s, nestedProp.properties(k))
                    case None => valArr(idx) = valArr(idx) :+ null
                  }
              }
            }
            InternalRow.fromSeq(
              valArr.map(seq => UTF8String.fromString(jsonMapper.writeValueAsString(seq))))
          case _ => throw new ElasticIndexException("WTF!? Non-nested-type field with nested value!?")
        }
      case v: Map[String, Any] =>
        property match {
          case prop: NestedProperty =>
            var valSeq = Seq[Any]()
            val nestedProp = prop.asInstanceOf[NestedProperty]
            nestedProp.properties.keys.foreach { k =>
              v.asInstanceOf[Map[String, Any]].get(k) match {
                case Some(s) if s.isInstanceOf[String] =>
                  valSeq = valSeq :+ UTF8String.fromString(s.asInstanceOf[String])
                case Some(ns) => valSeq = valSeq :+ processValue(ns, nestedProp.properties(k))
                case None => valSeq = valSeq :+ null
              }
            }
            InternalRow.fromSeq(valSeq)
          case _ => throw new ElasticIndexException("WTF!? Non-nested-type field with nested value!?")
        }
      case v: Seq[_] if v.isEmpty => null
      case v: Seq[_] =>
        InternalRow.fromSeq(v)
      case _ => v
    }
  }
}

object ElasticSearchResultIterator {

  def apply(relationInfo: ElasticRelationInfo,
             is: InputStream,
            onDone: => Unit = (),
            fromList: Boolean = false): CloseableIterator[SearchResultRow] = {

    if (fromList) {
      new ElasticSearchResultStaticIterator(relationInfo, is, onDone)
    } else {
      new ElasticSearchResultStreamingIterator(relationInfo, is, onDone)
    }
  }
}

class DummyResultIterator extends NextIterator[ResultRow] with CloseableIterator[ResultRow] {
  override protected def getNext(): ResultRow = {
    finished = true
    null
  }

  override protected def close(): Unit = ()
}
