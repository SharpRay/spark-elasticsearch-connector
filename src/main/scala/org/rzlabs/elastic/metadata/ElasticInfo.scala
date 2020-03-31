package org.rzlabs.elastic.metadata

case class ElasticOptions(host: String,
                          index: String,
                          `type`: String,
                          poolMaxConnectionsPerRoute: Int,
                          poolMaxConnections: Int)

