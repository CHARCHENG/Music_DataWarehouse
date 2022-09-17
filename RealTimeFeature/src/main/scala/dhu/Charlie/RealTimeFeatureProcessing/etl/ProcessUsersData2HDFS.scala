package dhu.Charlie.RealTimeFeatureProcessing.etl

import dhu.Charlie.RealTimeFeatureProcessing.bean.UsersClickMsg
import dhu.Charlie.RealTimeFeatureProcessing.serde.UsersClickMsgKafkaDeserializationSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object ProcessUsersData2HDFS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置和topic的consumer个数相同
    env.setParallelism(1)
    env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE)

    // 从Kafka中读取配置的
    val properties = new Properties()
    properties.put("bootstrap.servers", "Hadoop102:9092")
    properties.setProperty("group.id", "RideExercise")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("group.id", "consumer_test_group")

    val sources = env.addSource(new FlinkKafkaConsumer[UsersClickMsg](
      "test1",
      new UsersClickMsgKafkaDeserializationSchema(),
      properties
    ).setStartFromEarliest() // 指定从消费者组最开始
    )




  }
}