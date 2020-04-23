package org.rzlabs.elastic.metadata

import org.rzlabs.elastic.ElasticIndex

case class ElasticOptions(host: String,
                          index: String,
                          `type`: Option[String],
                          poolMaxConnectionsPerRoute: Int,
                          poolMaxConnections: Int,
                          cacheIndexMappings: Boolean,
                          skipUnknownTypeFields: Boolean,
                          debugTransformations: Boolean,
                          timeZoneId: String,
                          dateTypeFormats: Seq[String])

case class ElasticRelationInfo(name: String,
                               indexInfo: ElasticIndex,
                               options: ElasticOptions)
