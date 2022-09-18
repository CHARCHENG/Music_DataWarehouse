package dhu.Charlie.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadingParquetWithSparksql {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "charlie")
    val conf = new SparkConf().setMaster("local[*]").setAppName("ReadingParquetWithSparksql Job")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val hdfsPath = "hdfs://hadoop102:8020/RealTime_ODS_MUSIC_USERS"

    sparkSession.read.load(hdfsPath).createTempView("RealTime_ODS_MUSIC_USERS")

    sparkSession.sql(
      """
        |select
        |*
        |from RealTime_ODS_MUSIC_USERS
        |""".stripMargin).show()

    println("------------------all finished--------------------------")
  }
}
