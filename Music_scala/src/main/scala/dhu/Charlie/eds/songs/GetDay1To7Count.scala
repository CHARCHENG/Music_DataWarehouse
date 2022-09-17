package dhu.Charlie.eds.songs

import dhu.Charlie.utills.{ConfigUtils, DateUtils, LoadingDataFromESWithSparkSql}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object GetDay1To7Count {
  val hiveMetaStore = ConfigUtils.HIVE_METASTORE_URIS
  val dbName = ConfigUtils.HIVE_DATABASE

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("请输入要生成报表的日期;")
      System.exit(1)
    }
    val data: String = args(0)

    // 设置操作的用户名
    System.setProperty("HADOOP_USER_NAME", "charlie")

    // 在本地模式下部署 Sparksql on Hive
    val conf = new SparkConf().setMaster("local[*]").setAppName("DWD_Songs_Msg_Product")

    conf.set("es.index.auto.create", "true") //在spark中自动创建es中的索引
    conf.set("es.nodes", "localhost")//设置在spark中连接es的url和端口
    conf.set("es.port", "9200")
    conf.set("es.nodes.wan.only", "true")
    conf.set("hive.metastore.uris", hiveMetaStore)

    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    sparkSession.sql(s"use $dbName")

    LoadingDataFromESWithSparkSql.loadingDataFromEs(sparkSession, "ods_music_users_logs_msg", "to_client_song_play_operate_req_d")

    // 统计当天的点常量
    sparkSession.sql(
      s"""
         |select
         |songid as NBR,
         |songname,
         |0 as RCT_1_SUPP_CNT,
         |count(songid) as RCT_1_OrderTimes,
         |count(distinct uid) as RCT_1_UID_CNT,
         |count(distinct order_id) as RCT_1_ORDR_CNT
         |from to_client_song_play_operate_req_d
         |where to_date(datetimes) = '$data'
         |group by NBR, songname;
         |""".stripMargin).createTempView("TODAY_COUNT_TABLE")

    val earlyDay_7 = DateUtils.dateAdd(data, 7)

    // 统计7天内的点唱量
    sparkSession.sql(
      s"""
         |select songid as NBR,
         |0 as RCT_7_SUPP_CNT,
         |count(songid) as RCT_7_OrderTimes,
         |count(distinct uid) as RCT_7_UID_CNT,
         |count(distinct order_id) as RCT_7_ORDR_CNT
         |from to_client_song_play_operate_req_d
         |where to_date(datetimes) >= '$earlyDay_7' and to_date(datetimes) <= '$data'
         |group by NBR;
         |""".stripMargin).createTempView("DAY_7_COUNT_TABLE")

    val earlyDay_30 = DateUtils.dateAdd(data, 30);

    // 统计30天内的点唱量
    sparkSession.sql(
      s"""
         |select songid as NBR,
         |0 as RCT_30_SUPP_CNT,
         |count(songid) as RCT_30_OrderTimes,
         |count(distinct uid) as RCT_30_UID_CNT,
         |count(distinct order_id) as RCT_30_ORDR_CNT
         |from to_client_song_play_operate_req_d
         |where to_date(datetimes) >= '$earlyDay_30' and to_date(datetimes) <= '$data'
         |group by NBR;
         |""".stripMargin).createTempView("DAY_30_COUNT_TABLE")

    //从表中获取过去7天每首歌曲的 最高日点唱量
    sparkSession.sql(
      s"""
         | select NBR,
         | max(RCT_7_OrderTimes) as MAX_7_DAYCOUNT
         | from (
         |        select songid as NBR, count(songid) as RCT_7_OrderTimes
         |        from to_client_song_play_operate_req_d
         |        where to_date(datetimes) >= '$earlyDay_7' and to_date(datetimes) <= '$data'
         |        group by songid, to_date(datetimes)
         |) b group by NBR;
       """.stripMargin).createTempView("pre7InfoTable")
//
    //从表中获取过去30天每首歌曲的 最高日点唱量
    sparkSession.sql(
      s"""
         | select NBR,
         | max(RCT_30_OrderTimes) as MAX_30_DAYCOUNT
         | from (
         |        select songid as NBR, count(songid) as RCT_30_OrderTimes
         |        from to_client_song_play_operate_req_d
         |        where to_date(datetimes) >= '$earlyDay_30' and to_date(datetimes) <= '$data'
         |        group by songid, to_date(datetimes)
         | ) b group by NBR;
       """.stripMargin).createTempView("pre30InfoTable")

    sparkSession.table("pre30InfoTable").show()

    sparkSession.sql(s"use $dbName")

    // 将表信息进行汇总
    sparkSession.sql(
      s"""
         |select ts.NBR,
         |name,
         |source,
         |album,
         |prdct,
         |lang,
         |dur,
         |singer1,
         |singer1id,
         |singer2,
         |singer2id,
         |post_time,
         |RCT_1_SUPP_CNT,
         |RCT_1_OrderTimes,
         |RCT_1_UID_CNT,
         |RCT_1_ORDR_CNT,
         |RCT_7_SUPP_CNT,
         |RCT_7_OrderTimes,
         |RCT_7_UID_CNT,
         |RCT_7_ORDR_CNT,
         |RCT_30_SUPP_CNT,
         |RCT_30_OrderTimes,
         |RCT_30_UID_CNT,
         |RCT_30_ORDR_CNT,
         |MAX_7_DAYCOUNT,
         |MAX_30_DAYCOUNT
         |from tw_song_baseinfo_d ts
         |left join TODAY_COUNT_TABLE tc on ts.NBR = tc.NBR
         |left join DAY_7_COUNT_TABLE dca on ts.NBR = dca.NBR
         |left join DAY_30_COUNT_TABLE dcb on ts.NBR = dcb.NBR
         |left join pre7InfoTable pit on ts.NBR = pit.NBR
         |left join pre30InfoTable pft on ts.NBR = pft.NBR
         |""".stripMargin).write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("TW_SONG_FTUR_D")
  }
}
