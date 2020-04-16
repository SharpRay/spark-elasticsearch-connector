package org.apache.spark.sql.sources.elastic

trait CloseableIterator[+A] extends Iterator[A] {
  def closeIfNeeded(): Unit
}
