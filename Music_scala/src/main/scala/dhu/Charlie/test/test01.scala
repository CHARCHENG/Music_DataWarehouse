package dhu.Charlie.test

import dhu.Charlie.utills.ConfigUtils
import org.apache.spark.{SparkConf, SparkContext}

object test01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("generateLogFile")
    val sc = new SparkContext(sparkConf)
    val hdfsClientLogPath = ConfigUtils.HDFS_CLIENT_LOG_PATH
    val originData = sc.textFile("D:\\code\\music_project\\Music_scala\\src\\main\\data\\currentday_clientlog.tar.gz")
    val tempProcessData = originData.map(_.split("&")).filter(_.length == 6).map(line => (line(2), line(3)))
    print(tempProcessData.foreach(print))
  }
}
