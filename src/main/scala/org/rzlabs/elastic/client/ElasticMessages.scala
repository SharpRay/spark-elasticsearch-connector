package org.rzlabs.elastic.client

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}


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


