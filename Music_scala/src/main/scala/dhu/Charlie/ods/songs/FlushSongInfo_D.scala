package dhu.Charlie.ods.songs

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import dhu.Charlie.utills.{ConfigUtils, DateUtils, LoadingDataFromESWithSparkSql}
import org.apache.spark.SparkConf
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
    val dbName = ConfigUtils.HIVE_DATABASE

    // 在本地模式下部署 Sparksql on Hive
    val conf = new SparkConf().setMaster("local[*]").setAppName("DWD_Songs_Msg_Product")

    conf.set("es.index.auto.create", "true") //在spark中自动创建es中的索引
    conf.set("es.nodes", "localhost")//设置在spark中连接es的url和端口
    conf.set("es.port", "9200")
    conf.set("es.nodes.wan.only", "true")
    conf.set("hive.metastore.uris", hiveMetaStore)

    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    // 使用udf将函数类型参数getAlbumName转化为udf函数
    val udfGetAlbumName = udf(getAlbumName)
    val udfGetPostTime = udf(getPostTime)
    val udfProcessSingerInfo = udf(processSingerInfo)
    val udfGetAuthCompany = udf(getAuthCompany)
    val udfGetPublicMachineId = udf(getPublicMachineId)

    LoadingDataFromESWithSparkSql.loadingDataFromEs(sparkSession, "ods_songs_msg", "TO_SONG_INFO_D")

    sparkSession.table("TO_SONG_INFO_D")
      .withColumn("ALBUM", udfGetAlbumName(col("album")))
      .withColumn("POST_TIME", udfGetPostTime(col("post_time")))
      .withColumn("SINGER1", udfProcessSingerInfo(col("singer_info"), lit(1), lit("name")))
      .withColumn("SINGER1ID", udfProcessSingerInfo(col("singer_info"), lit(1), lit("id")))
      .withColumn("SINGER2", udfProcessSingerInfo(col("singer_info"), lit(2), lit("name")))
      .withColumn("SINGER2ID", udfProcessSingerInfo(col("singer_info"), lit(2), lit("id")))
      .withColumn("AUTH_CO", udfGetAuthCompany(col("authorized_company")))
      .withColumn("PRDCT_TYPE", udfGetPublicMachineId(col("publish_to")))
      .createTempView("TEMP_TO_SONG_INFO_D")

//
    /**
     * 清洗数据，将结果保存到 Hive TW_SONG_BASEINFO_D 表中
     */
    sparkSession.sql(s"use $dbName")

    sparkSession.sql(
      """
        |select
        |       source_id as NBR,
        |       nvl(NAME,OTHER_NAME) as NAME,
        |       source as SOURCE,
        |       ALBUM,
        |       product as PRDCT,
        |       language as LANG,
        |       video_format as VIDEO_FORMAT,
        |       duration as DUR,
        |       SINGER1,
        |       SINGER2,
        |       SINGER1ID,
        |       SINGER2ID,
        |       0 as MAC_TIME,
        |       POST_TIME,
        |       pinyin_first as PINYIN_FST,
        |       pinyin as PINYIN,
        |       singing_type as SING_TYPE,
        |       original_singer as ORI_SINGER,
        |       LYRICIST,
        |       COMPOSER,
        |       bpm as BPM_VAL,
        |       STAR_LEVEL,
        |       video_quality as VIDEO_QLTY,
        |       video_make as VIDEO_MK,
        |       video_feature as VIDEO_FTUR,
        |       lyric_feature as LYRIC_FTUR,
        |       Image_quality as IMG_QLTY,
        |       SUBTITLES_TYPE,
        |       audio_format as AUDIO_FMT,
        |       original_sound_quality as ORI_SOUND_QLTY,
        |       original_track_vol as ORI_TRK,
        |       original_sound_quality as ORI_TRK_VOL,
        |       accompany_version as ACC_VER,
        |       accompany_quality as ACC_QLTY,
        |       acc_track_vol as ACC_TRK_VOL,
        |       accompany_track as ACC_TRK,
        |       WIDTH,
        |       HEIGHT,
        |       video_resolution as VIDEO_RSVL,
        |       song_version as SONG_VER,
        |       authorized_company as AUTH_CO,
        |       status as STATE,
        |       case when size(PRDCT_TYPE) =0 then NULL else PRDCT_TYPE  end as PRDCT_TYPE
        |    from TEMP_TO_SONG_INFO_D
        |    where source_id != ''
        """.stripMargin)
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .format("parquet")
      .saveAsTable("TW_SONG_BASEINFO_D")

    println("------------------all finished--------------------------")
  }

}
