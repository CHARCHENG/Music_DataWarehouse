package dhu.Charlie.ods.songs

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import dhu.Charlie.utills.{ConfigUtils, DateUtils}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object FlushSongInfo_D {
  val hiveDataBase = ConfigUtils.HIVE_DATABASE
  val hiveMetaStore = ConfigUtils.HIVE_METASTORE_URIS
  /**
   * 获取专辑的名称
   *
   * @param albumInfo 专辑信息
   * @return
   */
  val getAlbumName: String => String = (albumInfo: String) => {
    var albumName = ""
    try {
      val jsonArray: JSONArray = JSON.parseArray(albumInfo)
      albumName = jsonArray.getJSONObject(0).getString("name")
    } catch {
      case e: Exception =>
        if (albumInfo.contains("《") && albumInfo.contains("》")) {
          albumName = albumInfo.substring(albumInfo.indexOf('《'), albumInfo.indexOf('》') + 1)
        } else {
          albumName = "暂无专辑"
        }
    }
    albumName
  }

  /**
   * 将时间标准化为yyyy-MM-dd HH:mm:ss
   *
   * @param time
   * @return
   */
  val getPostTime: String => String = (time: String) => {
    DateUtils.formatDate(time)
  }

  /**
   * 将歌手信息(包括翻唱和原唱和作曲以及作曲家)拆分为姓名和id
   *
   * @param singerInfo
   * @param singerOrder
   * @param needGet
   * @return
   */
  val processSingerInfo: (String, Int, String) => String = (singerInfo: String, singerOrder: Int, needGet: String) => {
    var res: String = ""
    try {
      val jsonArray = JSON.parseArray(singerInfo)
      if (singerOrder == 1 && "name".equals(needGet)) {
        res = jsonArray.getJSONObject(0).getString("name")
      }
      else if (singerOrder == 1 && "id".equals(needGet)) {
        res = jsonArray.getJSONObject(0).getString("id")
      }
      else if (singerOrder == 2 && "name".equals(needGet) && jsonArray.size() > 1) {
        res = jsonArray.getJSONObject(1).getString("name")
      }
      else if (singerOrder == 2 && "id".equals(needGet) && jsonArray.size() > 1) {
        res = jsonArray.getJSONObject(1).getString("id")
      }
    } catch {
      case e: Exception => {
        res
      }
    }
    res
  }

  /**
   * 将发布的机器信息转换
   *
   * @param publishTo
   * @return
   */

  val getPublicMachineId: String => ListBuffer[Int] = (publishTo: String) => {
    val r = new ListBuffer[Int]();
    if (!"".equals(publishTo.trim)) {
      val strings = publishTo.stripPrefix("[").stripSuffix("]").split(",")
      strings.foreach(t => {
        r.append(t.toDouble.toInt)
      })
    }
    r
  }

  /**
   * 获取 授权公司
   */
  val getAuthCompany: String => String = (authCompanyInfo: String) => {
    var authCompanyName = "乐心曲库"
    try {
      val jsonObject: JSONObject = JSON.parseObject(authCompanyInfo)
      authCompanyName = jsonObject.getString("name")
    } catch {
      case e: Exception => {
        authCompanyName
      }
    }
    authCompanyName
  }


  def main(args: Array[String]): Unit = {
    // 设置操作的用户名
    System.setProperty("HADOOP_USER_NAME", "charlie")
    // 在本地模式下部署 Sparksql on Hive
    val sparkSession = SparkSession.builder().master("local[*]").config("hive.metastore.uris", hiveMetaStore).enableHiveSupport().getOrCreate()

    // 使用udf将函数类型参数getAlbumName转化为udf函数
    val udfGetAlbumName = udf(getAlbumName)
    val udfGetPostTime = udf(getPostTime)
    val udfProcessSingerInfo = udf(processSingerInfo)
    val udfGetAuthCompany = udf(getAuthCompany)
    val udfGetPublicMachineId = udf(getPublicMachineId)


    sparkSession.sql(s"use $hiveDataBase")
    sparkSession.table("TO_SONG_INFO_D")
      .withColumn("ALBUM", udfGetAlbumName(col("ALBUM")))
      .withColumn("POST_TIME", udfGetPostTime(col("POST_TIME")))
      .withColumn("SINGER1", udfProcessSingerInfo(col("SINGER_INFO"), lit(1), lit("name")))
      .withColumn("SINGER1ID", udfProcessSingerInfo(col("SINGER_INFO"), lit(1), lit("id")))
      .withColumn("SINGER2", udfProcessSingerInfo(col("SINGER_INFO"), lit(2), lit("name")))
      .withColumn("SINGER2ID", udfProcessSingerInfo(col("SINGER_INFO"), lit(2), lit("id")))
      .withColumn("AUTH_CO", udfGetAuthCompany(col("AUTH_CO")))
      .withColumn("PRDCT_TYPE", udfGetPublicMachineId(col("PRDCT_TYPE")))
      .createTempView("TEMP_TO_SONG_INFO_D")


    /**
     * 清洗数据，将结果保存到 Hive TW_SONG_BASEINFO_D 表中
     */
    sparkSession.sql(
      """
        | select NBR,
        |       nvl(NAME,OTHER_NAME) as NAME,
        |       SOURCE,
        |       ALBUM,
        |       PRDCT,
        |       LANG,
        |       VIDEO_FORMAT,
        |       DUR,
        |       SINGER1,
        |       SINGER2,
        |       SINGER1ID,
        |       SINGER2ID,
        |       0 as MAC_TIME,
        |       POST_TIME,
        |       PINYIN_FST,
        |       PINYIN,
        |       SING_TYPE,
        |       ORI_SINGER,
        |       LYRICIST,
        |       COMPOSER,
        |       BPM_VAL,
        |       STAR_LEVEL,
        |       VIDEO_QLTY,
        |       VIDEO_MK,
        |       VIDEO_FTUR,
        |       LYRIC_FTUR,
        |       IMG_QLTY,
        |       SUBTITLES_TYPE,
        |       AUDIO_FMT,
        |       ORI_SOUND_QLTY,
        |       ORI_TRK,
        |       ORI_TRK_VOL,
        |       ACC_VER,
        |       ACC_QLTY,
        |       ACC_TRK_VOL,
        |       ACC_TRK,
        |       WIDTH,
        |       HEIGHT,
        |       VIDEO_RSVL,
        |       SONG_VER,
        |       AUTH_CO,
        |       STATE,
        |       case when size(PRDCT_TYPE) =0 then NULL else PRDCT_TYPE  end as PRDCT_TYPE
        |    from TEMP_TO_SONG_INFO_D
        |    where NBR != ''
      """.stripMargin).write.mode(SaveMode.Overwrite).saveAsTable("TW_SONG_BASEINFO_D")
    println("------------------all finished--------------------------")
  }

}
