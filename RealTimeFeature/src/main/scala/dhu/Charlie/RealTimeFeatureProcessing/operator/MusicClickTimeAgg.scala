package dhu.Charlie.RealTimeFeatureProcessing.operator

import dhu.Charlie.RealTimeFeatureProcessing.bean.UsersClickMsg
import org.apache.flink.api.common.functions.AggregateFunction

class MusicClickTimeAgg extends AggregateFunction[UsersClickMsg, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UsersClickMsg, acc: Long): Long = acc + 1L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
