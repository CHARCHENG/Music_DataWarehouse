package dhu.Charlie.RealTimeFeatureProcessing.jobs

import com.alibaba.fastjson.JSON
import com.ververica.cdc.connectors.mysql.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.{JsonDebeziumDeserializationSchema, StringDebeziumDeserializationSchema}
import dhu.Charlie.RealTimeFeatureProcessing.operator.SongsMsgFlatMap
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util

object LoadSongsMsg2ES {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setString(RestOptions.BIND_PORT, "8085") // 指定访问端口
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    env.setParallelism(1)
    env.enableCheckpointing(2 * 60 * 1000, CheckpointingMode.EXACTLY_ONCE)

    // 设置访问 HDFS 的用户名
    System.setProperty("HADOOP_USER_NAME", "Charlie")

    val indexName = "ods_songs_msg"

    // 创建 Flink-MySQL-CDC 的 Source
    val mysqlSource = MySqlSource.builder()
      .hostname("Hadoop102")
      .port(3306)
      .username("root")
      .password("123456")
      .databaseList("songdb")
      .tableList("songdb.song") //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据， 注意：指定的时候需要使用"db.table"的方式
      .startupOptions(StartupOptions.initial())
      .deserializer(new JsonDebeziumDeserializationSchema()).build()

    /**
    * flink cdc同步数据时的数据几种数据格式：
      * insert (op: "r")  :{"before":null,"after":{"id":30,"name":"wangsan","age":27,"address":"北京"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1652844463000,"snapshot":"false","db":"test","sequence":null,"table":"test_cdc","server_id":1,"gtid":null,"file":"mysql-bin.000001","pos":10525,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1652844463895,"transaction":null}
      * update :{"before":{"id":30,"name":"wangsan","age":27,"address":"北京"},"after":{"id":30,"name":"wangsan","age":27,"address":"上海"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1652844511000,"snapshot":"false","db":"test","sequence":null,"table":"test_cdc","server_id":1,"gtid":null,"file":"mysql-bin.000001","pos":10812,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1652844511617,"transaction":null}
      * delete :{"before":{"id":25,"name":"wanger","age":26,"address":"西安"},"after":null,"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1652844336000,"snapshot":"false","db":"test","sequence":null,"table":"test_cdc","server_id":1,"gtid":null,"file":"mysql-bin.000001","pos":10239,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1652844336733,"transaction":null}
    */

    val value = env.addSource(mysqlSource).flatMap(new SongsMsgFlatMap())

    // 将数据写入到ES
    val httpHosts = new util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("localhost", 9200, "http"))

    val elasticsearchSinkFunction = new ElasticsearchSinkFunction[String]() {
      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        val maps = JSON.parseObject(element, classOf[util.Map[String, Object]])
        val request = Requests.indexRequest.index(indexName).source(maps)
        indexer.add(request)
      }
    }

    value.addSink(new ElasticsearchSink.Builder[String](httpHosts, elasticsearchSinkFunction).build)

    env.execute("Flink CDC LoadingData")
  }
}
