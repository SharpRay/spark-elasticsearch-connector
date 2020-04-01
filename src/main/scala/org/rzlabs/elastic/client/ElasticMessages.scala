package org.rzlabs.elastic.client

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty, JsonSubTypes, JsonTypeInfo}
import org.rzlabs.elastic.ElasticDataType

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes(Array(
  new JsonSubTypes.Type(value = classOf[TextProperty], name = "text"),
  new JsonSubTypes.Type(value = classOf[KeywordProperty], name = "keyword"),
  new JsonSubTypes.Type(value = classOf[DateProperty], name = "date"),
  new JsonSubTypes.Type(value = classOf[LongProperty], name = "long"),
  new JsonSubTypes.Type(value = classOf[IntegerProperty], name = "integer"),
  new JsonSubTypes.Type(value = classOf[ShortProperty], name = "short"),
  new JsonSubTypes.Type(value = classOf[ByteProperty], name = "byte"),
  new JsonSubTypes.Type(value = classOf[DoubleProperty], name = "double"),
  new JsonSubTypes.Type(value = classOf[FloatProperty], name = "float"),
  new JsonSubTypes.Type(value = classOf[BooleanProperty], name = "boolean"),
  new JsonSubTypes.Type(value = classOf[NestedProperty], name = "nested"),
))
sealed trait IndexProperty extends Serializable {
  def dataType: ElasticDataType.Value
}

case class TextProperty(fields: Map[String, IndexProperty],
                        analyzer: String) extends IndexProperty {

  override def dataType = ElasticDataType.Text

  def keywordFields(): Seq[String] = {
    if (fields == null) {
      Seq[String]()
    } else {
      fields.filter(field => field._2.isInstanceOf[KeywordProperty]).keys.toSeq
    }
  }
}
case class KeywordProperty() extends IndexProperty {
  override def dataType = ElasticDataType.Keyword
}
case class DateProperty() extends IndexProperty {
  override def dataType = ElasticDataType.Date
}
case class LongProperty() extends IndexProperty {
  override def dataType = ElasticDataType.Long
}
case class IntegerProperty() extends IndexProperty {
  override def dataType = ElasticDataType.Int
}
case class ShortProperty() extends IndexProperty {
  override def dataType = ElasticDataType.Short
}
case class ByteProperty() extends IndexProperty {
  override def dataType = ElasticDataType.Byte
}
case class DoubleProperty() extends IndexProperty {
  override def dataType = ElasticDataType.Double
}
case class FloatProperty() extends IndexProperty {
  override def dataType = ElasticDataType.Float
}
case class BooleanProperty() extends IndexProperty {
  override def dataType = ElasticDataType.Boolean
}
case class NestedProperty(properties: Map[String, IndexProperty]) extends IndexProperty {
  override def dataType = ElasticDataType.Nested
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class ClusterStatus(@JsonProperty("cluster_name") clusterName: String,
                         status: String,
                         @JsonProperty("time_out") timeOut: String,
                         @JsonProperty("number_of_nodes") numberOfNodes: Int,
                         @JsonProperty("number_of_data_nodes") numberOfDataNodes: Int,
                         @JsonProperty("active_primary_shards") activePrimaryShards: Int,
                         @JsonProperty("active_shards") activeShards: Int,
                         @JsonProperty("relocating_shards") relocatiingShards: Int,
                         @JsonProperty("initializing_shards") initializingShards: Int,
                         @JsonProperty("unassigned_shards") unassignedShards: Int,
                         @JsonProperty("delayed_unassigned_shards") delayedUnassignedShards: Int,
                         @JsonProperty("number_of_pending_tasks") numberOfPendingTasks: Int,
                         @JsonProperty("number_of_in_flight_fetch") numberOfInFlightFetch: Int,
                         @JsonProperty("task_max_waiting_in_queue_millis") taskMaxWaitingInQueueMillis: Int,
                         @JsonProperty("active_shards_percent_as_number") activeShardsPercentAsNumber: Float
                        )

case class IndexMappings(mappings: Map[String, IndexProperties])

case class IndexProperties(properties: Map[String, IndexProperty])