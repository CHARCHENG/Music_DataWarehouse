package dhu.Charlie.RealTimeFeatureProcessing.etl

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import dhu.Charlie.RealTimeFeatureProcessing.bean.UsersClickMsg
import dhu.Charlie.RealTimeFeatureProcessing.serde.UsersClickMsgKafkaSerializationSchema
import dhu.Charlie.RealTimeFeatureProcessing.sinks.MyClickHouseSink
import dhu.Charlie.RealTimeFeatureProcessing.utils.ProductRandomTime
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.elasticsearch.action.index.IndexRequest
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.elasticsearch.client.Requests
import org.apache.http.HttpHost

import java.util
import java.util.Properties

object ProcessUsersData2ClickHouseAndEsAndKafka {

  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val conf = new Configuration()
    conf.setString(RestOptions.BIND_PORT, "8083") // 指定访问端口
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    env.setParallelism(1)
    env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE)
    var path = "D:\\大学&研究生\\电子书\\音乐中心\\need\\datas\\"
    val sql = "insert into music_users_logs.t_music_users_logs values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    if (args.length < 1) {
      println("请输入要生成报表的日期;")
      System.exit(1)
    }
    val dates: String = args(0)

    path = path.concat(dates)
    val sources = env.readTextFile(path)
    println("=====================" + dates + "==============================")
    var uid = 0
    val value = sources.map(x => x.split("&"))
        .filter(x => "MINIK_CLIENT_SONG_PLAY_OPERATE_REQ".equals(x(2)))
        .map(x => {
          val nObject = JSON.parseObject(x(3))
          uid = uid + 1
          new UsersClickMsg(
            "mu_".concat(dates + "_").concat(uid.toString),
            nObject.getString("songid"),
            nObject.getString("mid").toInt,
            nObject.getString("optrate_type").toInt,
            nObject.getString("uid").toLong,
            nObject.getString("consume_type").toInt,
            nObject.getString("play_time").toInt,
            nObject.getString("dur_time").toInt,
            nObject.getString("session_id").toInt,
            nObject.getString("songname"),
            nObject.getString("pkg_id").toInt,
            nObject.getString("order_id"),
            ProductRandomTime.getFormatRandomTime(dates.concat(" 13:00:00"), dates.concat(" 17:30:00")))
        }
      )

//    // 将数据写入到Clickhouse
//    value.addSink(new MyClickHouseSink(sql))
//
//    // 将数据写入到ES
//    val httpHosts = new util.ArrayList[HttpHost]
//    httpHosts.add(new HttpHost("localhost", 9200, "http"))
//
//    val elasticsearchSinkFunction = new ElasticsearchSinkFunction[UsersClickMsg]() {
//      override def process(element: UsersClickMsg, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
//        val str = JSON.toJSON(element)
//        val maps = JSON.parseObject(str.toString, classOf[util.Map[String, Object]])
//        val request = Requests.indexRequest.index("ods_music_users_logs_msg").source(maps)
//        indexer.add(request)
//      }
//    }
//
//    value.addSink(new ElasticsearchSink.Builder[UsersClickMsg](httpHosts, elasticsearchSinkFunction).build)

    // 存入Kadka
    val properties = new Properties()
    properties.put("bootstrap.servers", "hadoop104:9092")
    properties.put("group.id", "consumer_test_group")

//    // Way1: 使用自定义的序列化类；
//    value.addSink(new FlinkKafkaProducer[UsersClickMsg](
//      "test1",
//      new UsersClickMsgKafkaSerializationSchema(),
//      properties,
//      FlinkKafkaProducer.Semantic.EXACTLY_ONCE
//    ))

    // Way2：先将自定义类转化为Json字符串形式再发送到Kafka
    value
      .map(x => JSON.toJSON(x).toString)
      .addSink(new FlinkKafkaProducer[String](
        "test1",
        new SimpleStringSchema(),
        properties
      ))

    env.execute("ProcessData2ClickHouse")
  }
}
