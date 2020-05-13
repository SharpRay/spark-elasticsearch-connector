package org.apache.spark.sql.catalyst.elastic.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.{ShuffledRowRDD, SparkPlan, UnaryExecNode, UnsafeRowSerializer}

case class CollectOffsetExec(offset: Int, limit: Int, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = SinglePartition
  override def executeCollect(): Array[InternalRow] = limit match {
    case -1 => child.executeCollect()
    case _ => child.executeTake(offset + limit)
  }
  private val serializer: Serializer = new UnsafeRowSerializer(child.output.size)
  protected override def doExecute(): RDD[InternalRow] = {
    def slice(iter: Iterator[InternalRow]) = limit match {
      case -1 => iter.drop(offset)
      case _ => iter.slice(offset, offset + limit)
    }
    def take(iter: Iterator[InternalRow]) = limit match {
      case -1 => iter
      case _ => iter.take(limit)
    }
    val locallyLimited = child.execute().mapPartitionsInternal(slice)
    val shuffled = new ShuffledRowRDD(
      ShuffleExchangeExec.prepareShuffleDependency(
        locallyLimited, child.output, SinglePartition, serializer))
    shuffled.mapPartitionsInternal(take)
  }
}

