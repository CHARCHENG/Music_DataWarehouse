package dhu.Charlie.dwd

import dhu.Charlie.utills.{ConfigUtils, DateUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

object GetDay1To7Count {
  val hiveMetastoreUris = ConfigUtils.HIVE_METASTORE_URIS
  val dbName = "music"
  def main(args: Array[String]): Unit = {
    if(args.length < 1){
      println("请输入要生成报表的日期;")
      System.exit(1)
    }
    val data: String = args(0)
    // 设置操作的用户名
    System.setProperty("HADOOP_USER_NAME", "charlie")
    val sparkSession = SparkSession.builder().master("local[*]").config("hive.metastore.uris", hiveMetastoreUris).enableHiveSupport().getOrCreate()
    sparkSession.sql(s"use $dbName")
    sparkSession.table("to_client_song_play_operate_req_d").createTempView("to_client_song_play_operate_req_d")

    // 统计当天的点常量
    sparkSession.sql(
      s"""
         |select songid as NBR,
         |0 as RCT_1_SUPP_CNT,
         |count(songid) as RCT_1_OrderTimes,
         |count(distinct uid) as RCT_1_UID_CNT,
         |count(distinct order_id) as RCT_1_ORDR_CNT
         |from to_client_song_play_operate_req_d
         |where data_dt = $data
         |group by songid;
         |""".stripMargin).createTempView("TODAY_COUNT_TABLE")

    val earlyDay_7 = DateUtils.dateAdd(data, 7);
    // 统计7天内的点唱量
    sparkSession.sql(
          s"""
             |select songid as NBR,
             |0 as RCT_7_SUPP_CNT,
             |count(songid) as RCT_7_OrderTimes,
             |count(distinct uid) as RCT_7_UID_CNT,
             |count(distinct order_id) as RCT_7_ORDR_CNT
             |from to_client_song_play_operate_req_d
             |where data_dt >= $earlyDay_7 and data_dt <= $data
             |group by songid;
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
             |where data_dt >= $earlyDay_30 and data_dt <= $data
             |group by songid;
             |""".stripMargin).createTempView("DAY_30_COUNT_TABLE")

    //从表中获取过去7天每首歌曲的 最高日点唱量
    sparkSession.sql(
      s"""
         | select NBR, max(RCT_7_OrderTimes) as MAX_7_DAYCOUNT from (select songid as NBR, count(songid) as RCT_7_OrderTimes
         | from to_client_song_play_operate_req_d
         | where data_dt >= $earlyDay_7 and data_dt <= $data
         | group by songid, data_dt) b group by NBR;
       """.stripMargin).createTempView("pre7InfoTable")

    //从表中获取过去30天每首歌曲的 最高日点唱量
    sparkSession.sql(
      s"""
         | select NBR, max(RCT_7_OrderTimes) as MAX_30_DAYCOUNT from (select songid as NBR, count(songid) as RCT_7_OrderTimes
         | from to_client_song_play_operate_req_d
         | where data_dt >= $earlyDay_30 and data_dt <= $data
         | group by songid, data_dt) b group by NBR;
       """.stripMargin).createTempView("pre30InfoTable")

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
         |from tw_song_baseinfo_d ts left join TODAY_COUNT_TABLE tc on ts.NBR = tc.NBR
         |left join DAY_7_COUNT_TABLE dca on ts.NBR = dca.NBR
         |left join DAY_30_COUNT_TABLE dcb on ts.NBR = dcb.NBR
         |left join pre7InfoTable pit on ts.NBR = pit.NBR
         |left join pre30InfoTable pft on ts.NBR = pft.NBR
         |""".stripMargin).write.mode(SaveMode.Overwrite).saveAsTable("TW_SONG_FTUR_D")
  }
}
