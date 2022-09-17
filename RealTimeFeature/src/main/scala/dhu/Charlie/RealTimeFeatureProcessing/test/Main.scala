package dhu.Charlie.RealTimeFeatureProcessing.test

import dhu.Charlie.RealTimeFeatureProcessing.utils.ProductRandomTime

import java.text.SimpleDateFormat

object Main {
  def main(args: Array[String]): Unit = {
    val times = ProductRandomTime.getRandomTimeStamp("2021-11-01 14:04:00", "2021-11-01 14:14:00")
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println(sdf.format(times))
  }
}
