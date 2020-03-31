package org.rzlabs.elastic

class ElasticIndexException(message: String, cause: Throwable)
    extends Exception(message, cause) {
  def this(message: String) = this(message, null)
}
