package dhu.Charlie.RealTimeFeatureProcessing.jobs

import dhu.Charlie.RealTimeFeatureProcessing.bean.UsersClickMsg
import dhu.Charlie.RealTimeFeatureProcessing.operator.CustomizedBucketAssigner
import dhu.Charlie.RealTimeFeatureProcessing.serde.UsersClickMsgKafkaDeserializationSchema
import dhu.Charlie.RealTimeFeatureProcessing.utils.BucketPatternUtils
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink

import java.util.Properties

/**
 * 将Kafka中的数据以parquet的形式分桶存入到HDFS中
 * @author Charlie.Huang
 */

object ProcessUsersData2HDFS {
  def main(args: Array[String]): Unit = {
    //val env = StreamExecutionEnvironment.getExecutionEnvironment

    System.setProperty("HADOOP_USER_NAME", "charlie")

    val conf = new Configuration()
    conf.setString(RestOptions.BIND_PORT, "8083") // 指定访问端口
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

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

    var patternFormat: String = null

    // 设置分区策略
    val dateTimeBucketPattern = "HOURLY"

    if ("DAILY".equals(dateTimeBucketPattern)){
      patternFormat = BucketPatternUtils.PATTERN_DW_DAILY
    }
    else if ("HOURLY".equals(dateTimeBucketPattern)){
      patternFormat = BucketPatternUtils.PATTERN_DW_HOURLY
    }
    else {
      patternFormat = BucketPatternUtils.PATTERN_DW_DAILY
    }

    val values = sources.filter(x => x != null)

    val hdfsSuffix = "RealTime_ODS_MUSIC_USERS"

    val hdfsPath = "hdfs://hadoop102:8020/" + hdfsSuffix

    val fileSink = StreamingFileSink.forBulkFormat(
      new Path(hdfsPath),
      ParquetAvroWriters.forReflectRecord(classOf[UsersClickMsg]))
      .withBucketAssigner(new CustomizedBucketAssigner(patternFormat))
      .build

    values.addSink(fileSink)

    env.execute("UsersData2HDFS Job")
  }
}