package dhu.Charlie.RealTimeFeatureProcessing.operator

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import dhu.Charlie.RealTimeFeatureProcessing.bean.MusicClickCountRes
import org.apache.flink.util.Collector


class MusicAccumlator2Window extends ProcessWindowFunction[Long, MusicClickCountRes, String, TimeWindow]{
  override def process(songname: String, context: Context, elements: Iterable[Long], out: Collector[MusicClickCountRes]): Unit = {

    val start = context.window.getStart
    val end = context.window.getEnd

    out.collect(new MusicClickCountRes(songname, start, end, elements.iterator.next.toInt))
  }
}
