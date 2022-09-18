package dhu.Charlie.RealTimeFeatureProcessing.test

import dhu.Charlie.RealTimeFeatureProcessing.utils.{ProductRandomTime, TimeUtils}

import java.text.SimpleDateFormat

object Main {
  def main(args: Array[String]): Unit = {
    println(TimeUtils.formatTimestamps(System.currentTimeMillis()))
  }
}
