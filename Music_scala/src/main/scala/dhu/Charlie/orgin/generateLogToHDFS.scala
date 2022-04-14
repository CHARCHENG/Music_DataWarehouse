package dhu.Charlie.orgin

import com.alibaba.fastjson.JSON
import dhu.Charlie.utills.ConfigUtils
import org.apache.spark.{SparkConf, SparkContext}

object generateLogToHDFS {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      print("请输入日期！")
      System.exit(1)
    } else {
      val logDate = args(0)
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("generateLogFile")
      val sc = new SparkContext(sparkConf)
      val hdfsClientLogPath = ConfigUtils.HDFS_CLIENT_LOG_PATH
      val originData = sc.textFile(s"D:\\大学&研究生\\电子书\\音乐中心\\need\\datas\\$logDate")
      val tempProcessData = originData.map(_.split("&")).filter(_.length == 6).map(line => (line(2), line(3)))
      tempProcessData.filter(_._1.equals("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ")).map(line => {
        val jsonValue = line._2
        val jsonObject = JSON.parseObject(jsonValue)
        val songid = jsonObject.getString("songid") //歌曲ID
        val mid = jsonObject.getString("mid") //机器ID
        val optrateType = jsonObject.getString("optrate_type") //操作类型 0:点歌, 1:切歌,2:歌曲开始播放,3:歌曲播放完成,4:录音试听开始,5:录音试听切歌,6:录音试听完成
        val uid = jsonObject.getString("uid") //用户ID（无用户则为0）
        val consumeType = jsonObject.getString("consume_type") //消费类型：0免费；1付费
        val durTime = jsonObject.getString("dur_time") //总时长单位秒（operate_type:0时此值为0）
        val sessionId = jsonObject.getString("session_id") //局数ID
        val songName = jsonObject.getString("songname") //歌曲名
        val pkgId = jsonObject.getString("pkg_id") //套餐ID类型
        val orderId = jsonObject.getString("order_id") //订单号
        songid + "\t" + mid + "\t" + optrateType + "\t" + uid + "\t" + consumeType + "\t" + durTime + "\t" + sessionId + "\t" + songName + "\t" + pkgId + "\t" + orderId
      }
      ).repartition(1).saveAsTextFile(
        s"${hdfsClientLogPath}/all_client_tables/$logDate",
      )
    }
  }
}
