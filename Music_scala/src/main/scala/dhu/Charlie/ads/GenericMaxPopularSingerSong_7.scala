package dhu.Charlie.ads

import dhu.Charlie.utills.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object GenericMaxPopularSingerSong_7 {
  val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  val hiveDataBases = ConfigUtils.HIVE_DATABASE
  val mysqlUrl = ConfigUtils.MYSQL_URL
  val mysqlUser = ConfigUtils.MYSQL_USER
  val mysqlPawwWord = ConfigUtils.MYSQL_PASSWORD
  def main(args: Array[String]): Unit = {
    // 设置操作的用户名
    System.setProperty("HADOOP_USER_NAME", "charlie")
    val sparkSession = SparkSession.builder().master("local[*]").config("hive.metastore.uris", hiveMetaStoreUris).enableHiveSupport().getOrCreate()
    sparkSession.sql(s"use $hiveDataBases")
    sparkSession.table("tw_song_ftur_d").createTempView("tw_song_ftur_d")

    // 统计七天内最热门的歌曲Top30
    sparkSession.sql(
      s"""
         |select '20220401' as dates, nbr, name,
         |       pow(0.8 * (0.63 * log(rct_7_ordertimes / 7 + 1) + (0.37 * log(max_7_daycount + 1))) + 0.2 * log(rct_7_supp_cnt / 7 + 1), 2) * 10 as index,
         |       singer1,
         |       singer2
         |from tw_song_ftur_d
         |where rct_7_ordertimes is not null
         |order by index desc limit 30;
         |""".stripMargin).createTempView("TM_DAY_7_SONGRANK_20220401_0")

    sparkSession.sql(
      s"""
         |select *, dense_rank() over (order by index desc) as rank
         |from TM_DAY_7_SONGRANK_20220401_0;
         |""".stripMargin).createTempView("TM_DAY_7_SONGRANK_20220401")

    // 统计七天内最热门的歌手Top30
    sparkSession.sql(
      s"""
         |select '20220401' as dates, singer1,
         |pow(0.8 * (0.63 * log(sum(rct_7_ordertimes) / 7 + 1) + 0.37 * log(max(max_7_daycount) + 1)) + 0.2 * (log(sum(rct_7_supp_cnt) / 7 + 1)), 2) * 10 as index
         |from tw_song_ftur_d
         |where rct_7_ordertimes is not null
         |group by singer1
         |order by index desc limit 30;
         |""".stripMargin).createTempView("TM_DAY_7_SINGERRANK_20220401_0")

    sparkSession.sql(
      s"""
         |select *, dense_rank() over (order by index desc) as rank
         |from TM_DAY_7_SINGERRANK_20220401_0;
         |""".stripMargin).createTempView("TM_DAY_7_SINGERRANK_20220401")

    sparkSession.table("TM_DAY_7_SONGRANK_20220401").write.format("Hive").mode(SaveMode.Overwrite).saveAsTable("TM_DAY_7_SONGRANK_20220401")
    sparkSession.table("TM_DAY_7_SINGERRANK_20220401").write.format("Hive").mode(SaveMode.Overwrite).saveAsTable("TM_DAY_7_SINGERRANK_20220401")

    val properties = new Properties()
    properties.setProperty("user", mysqlUser)
    properties.setProperty("password", mysqlPawwWord)
    properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    sparkSession.table("TM_DAY_7_SONGRANK_20220401").write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "TM_DAY_7_SONGRANK_20220401", properties)
    sparkSession.table("TM_DAY_7_SINGERRANK_20220401").write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "TM_DAY_7_SINGERRANK_20220401", properties)

  }
}
