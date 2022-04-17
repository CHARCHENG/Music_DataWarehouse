package dhu.Charlie.test

import dhu.Charlie.ods.songs.FlushSongInfo_D.hiveMetaStore
import dhu.Charlie.utills.{ConfigUtils, DateUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object test01 {
  def main(args: Array[String]): Unit = {
    val hiveDataBase = ConfigUtils.HIVE_DATABASE
    val sparkSession = SparkSession.builder().master("local[*]").config("hive.metastore.uris",hiveMetaStore).enableHiveSupport().getOrCreate()
    sparkSession.sql(s"use $hiveDataBase")
    sparkSession.table("TO_CLIENT_SONG_PLAY_OPERATE_REQ_D").show()
  }
}
