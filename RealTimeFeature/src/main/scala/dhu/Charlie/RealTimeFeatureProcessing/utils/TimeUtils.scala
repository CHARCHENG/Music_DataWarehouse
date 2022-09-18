package dhu.Charlie.RealTimeFeatureProcessing.utils

import java.text.SimpleDateFormat

object TimeUtils {

  def getTimestamps(time: String, formats: String = "yyyy-MM-dd HH:mm:ss"): Long = {
     val sdf = new SimpleDateFormat(formats);
     sdf.parse(time).getTime
  }

  def formatTimestamps(timestamps: Long, formats: String = "yyyy-MM-dd HH:mm:ss"): String = {
    val sdf = new SimpleDateFormat(formats);
    sdf.format(timestamps)
  }

}
