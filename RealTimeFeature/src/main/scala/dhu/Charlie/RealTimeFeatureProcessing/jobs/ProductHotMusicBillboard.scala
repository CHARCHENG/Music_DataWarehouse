package dhu.Charlie.RealTimeFeatureProcessing.jobs

import dhu.Charlie.RealTimeFeatureProcessing.bean.UsersClickMsg
import dhu.Charlie.RealTimeFeatureProcessing.operator.{MusicAccumlator2Window, MusicClickTimeAgg, MusicKeyedProcessFunc}
import dhu.Charlie.RealTimeFeatureProcessing.serde.UsersClickMsgKafkaDeserializationSchema
import dhu.Charlie.RealTimeFeatureProcessing.sinks.Flink2MySqlSink
import dhu.Charlie.RealTimeFeatureProcessing.utils.TimeUtils
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.time.Duration
import java.util.Properties

object ProductHotMusicBillboard {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setString(RestOptions.BIND_PORT, "8091") // 指定访问端口
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    env.setParallelism(1)
    env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE)

    // 设置kafka配置
    val properties = new Properties()

    val topicName = "test02"

    properties.put("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "RideExercise")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("group.id", "consumer_music_users_logs_msg_group")

    val sources = env.addSource(new FlinkKafkaConsumer[UsersClickMsg](
      topicName,
      new UsersClickMsgKafkaDeserializationSchema(),
      properties).setStartFromEarliest()
    ).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[UsersClickMsg](){
          override def extractTimestamp(t: UsersClickMsg, l: Long): Long = {
              TimeUtils.getTimestamps(t.getDatetimes)
          }
        })
    )

    val musicClickTime = sources
      .keyBy(x => x.getSongname)
      .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
      .aggregate(new MusicClickTimeAgg, new MusicAccumlator2Window)

    val topN = 10

    val resList = musicClickTime.keyBy(_.getEndTime).process(new MusicKeyedProcessFunc(topN))

    // mysql配置方式
    val host = "hadoop102"
    val port = "3306"
    val databases = "songdb"
    val table = "music_billboard"
    val users = "root"
    val password = "123456"

    resList.addSink(new Flink2MySqlSink(host, port, databases, table, users, password))

    env.execute("ProductHotMusicBillboard Job")
  }

}
