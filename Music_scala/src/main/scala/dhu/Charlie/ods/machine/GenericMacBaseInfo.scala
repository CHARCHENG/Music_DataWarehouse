package dhu.Charlie.ods.machine

import dhu.Charlie.utills.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Properties

object GenericMacBaseInfo {
  val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  val mysqlUrl = ConfigUtils.MYSQL_URL
  val mysqlUser = ConfigUtils.MYSQL_USER
  val mysqlPassword = ConfigUtils.MYSQL_PASSWORD
  val dbName = ConfigUtils.HIVE_DATABASE
  /**
   * udf 匿名函数，格式化时间，格式化成对应的 xxxxxxxxxxxxxx 年月日时分秒格式
   */
  val getFormatTime = (time:String) =>{
    var formatTime = time
    if(time.length<14 && time.length ==8){
      formatTime = time + "000000"
    }else{
      formatTime = "19700101000000"
    }
    formatTime
  }

  /**
   * udf 格式化省份
   */
  val getPrvcInfo = (prvc:String, addr:String) =>{
    var province = ""
    if(prvc == "null"){
      if(addr != null){
        province = getProvince(addr)
      }
    }else{
      province = prvc
    }
    province
  }

  /**
   * udf 格式化 城市和区
   */

  val getCtyInfo = (cty:String,addr:String) =>{
    var city = ""
    if(cty == "null") {
      if (addr != null) {
        city = getCity(addr)
      }
    }
    city
  }

  def getCity(addr:String) = {
    var returnCity = ""
    if(addr.contains("内蒙古自治区")){
      returnCity = addr.substring(addr.indexOf("区")+1,addr.indexOf("市"))
    }else if(addr.contains("宁夏回族自治区")){
      returnCity = addr.substring(addr.indexOf("区")+1,addr.indexOf("市"))
    }else if(addr.contains("广西壮族自治区")){
      returnCity = addr.substring(addr.indexOf("区")+1,addr.indexOf("市"))
    }else if(addr.contains("新疆维吾尔自治区")){
      try{
        returnCity = addr.substring(addr.indexOf("区")+1,addr.indexOf("市"))
      }catch{
        case e:Exception =>{
          val index = addr.indexOf("区")
          if(addr.substring(index+1,addr.length).contains("区")){
            returnCity = addr.substring(index+1,addr.indexOf("区",index+1))
          }else{
            returnCity = addr.substring(index+1,addr.indexOf("州",index+1))
          }
        }
      }
    }else if(addr.contains("北京市")){
      returnCity = addr.substring(addr.indexOf("市")+1,addr.indexOf("区"))
    }else if(addr.contains("上海市")){
      returnCity = addr.substring(addr.indexOf("市")+1,addr.indexOf("区"))
    }else if(addr.contains("重庆市")){
      val index = addr.indexOf("市")
      if(addr.substring(index+1,addr.length).contains("区")){
        returnCity = addr.substring(addr.indexOf("市")+1,addr.indexOf("区"))
      }else if(addr.substring(index+1,addr.length).contains("县")){
        returnCity = addr.substring(addr.indexOf("市")+1,addr.indexOf("县"))
      }

    }else if(addr.contains("天津市")){
      returnCity = addr.substring(addr.indexOf("市")+1,addr.indexOf("区"))
    }else if(addr.contains("省")){
      val index = addr.indexOf("省")
      if(addr.substring(index+1,addr.length).contains("市")){
        returnCity = addr.substring(index+1,addr.indexOf("市"))
      }else if(addr.substring(index+1,addr.length).contains("州")){
        returnCity = addr.substring(index+1,addr.substring(index+1,addr.length).indexOf("州"))
      }
      returnCity
    }
    returnCity
  }

  def getProvince(addr:String):String = {
    var returnProvince = ""
    if(addr.contains("内蒙古自治区")){
        returnProvince ="内蒙古"
    }else if(addr.contains("宁夏回族自治区")){
        returnProvince ="宁夏"
    }else if(addr.contains("西藏自治区")){
        returnProvince ="西藏"
    }else if(addr.contains("广西壮族自治区")){
        returnProvince ="广西"
    }else if(addr.contains("新疆维吾尔自治区")){
      returnProvince ="新疆"
    }else if(addr.contains("北京市")){
      returnProvince ="北京"
    }else if(addr.contains("上海市")){
      returnProvince ="上海"
    }else if(addr.contains("重庆市")){
      returnProvince ="重庆"
    }else if(addr.contains("天津市")){
      returnProvince ="天津"
    }else if(addr.contains("省")){
      returnProvince = addr.substring(0,addr.indexOf("省"))
    }
    returnProvince
  }

  def main(args: Array[String]): Unit = {
    // 设置操作的用户名
    System.setProperty("HADOOP_USER_NAME", "charlie")
    val sparkSession = SparkSession.builder().master("local[*]").config("hive.metastore.uris",hiveMetaStoreUris).enableHiveSupport().getOrCreate()
    sparkSession.sql(s"use $dbName")
    /**
     * 将函数注册为udf函数
     */
    val udfGetFormatTime = udf(getFormatTime)
    val udfGetPrvcInfo = udf(getPrvcInfo)
    val udfGetCtyInfo = udf(getCtyInfo)

    //加载 TO_YCAK_MAC_D 机器基本信息表 获取机器信息，并设置产品类型为2
    val frame1 = sparkSession.table("to_ycak_mac_d").withColumn("PRDCT_TYPE", lit(2))
    frame1.createTempView("TO_YCAK_MAC_D")

    //加载 TO_YCBK_MAC_ADMIN_MAP_D 机器客户关系资料表 获取机器信息,并将“ACTV_TM”进行格式化清洗和Null值的填补。
    val frame2 = sparkSession.table("TO_YCBK_MAC_ADMIN_MAP_D")
    frame2.withColumn("ACTV_TM",udfGetFormatTime(col("ACTV_TM"))).createTempView("TO_YCBK_MAC_ADMIN_MAP_D")

    frame1.select("MID","PRDCT_TYPE").union(frame2.select("MID","PRDCT_TYPE")).distinct().createTempView("TEMP_MAC_ALL")

    // 读取 ycak 库下的 抽取到的  TO_YCAK_MAC_LOC_D 机器位置信息表 进行清洗
    val prvcOrCtyIsNotNullDF = sparkSession.table("TO_YCAK_MAC_LOC_D").filter("prvc != 'null' and cty != 'null'")

    sparkSession.table("TO_YCAK_MAC_LOC_D")
      .filter("prvc = 'null' and cty = 'null'")
      .withColumn("PRVC",udfGetPrvcInfo(col("PRVC"),col("ADDR")))
      .withColumn("CTY",udfGetCtyInfo(col("CTY"),col("ADDR")))
      .union(prvcOrCtyIsNotNullDF)
      .filter("prvc != '' and cty != ''")
      .withColumn("REV_TM",udfGetFormatTime(col("REV_TM")))
      .withColumn("SALE_TM",udfGetFormatTime(col("SALE_TM")))
      .createTempView("TO_YCAK_MAC_LOC_D")

    /**
     * 获取 机器上歌曲版本 机器位置信息, 以 TEMP_MAC_ALL 中的MID 为基准，对 TO_YCAK_MAC_D、TO_YCAK_MAC_LOC_D两张表的信息进行统计
     */
    sparkSession.sql(
      """SELECT
        |   TEMP.MID,
        |   MAC.SRL_ID,
        |   MAC.HARD_ID,
        |   MAC.SONG_WHSE_VER,
        |   MAC.EXEC_VER,
        |   MAC.UI_VER,
        |   MAC.STS,
        |   MAC.CUR_LOGIN_TM,
        |   MAC.PAY_SW,
        |   MAC.IS_ONLINE,
        |   MAC.PRDCT_TYPE,
        |   LOC.PRVC,
        |   LOC.CTY,
        |   LOC.ADDR_FMT,
        |   LOC.REV_TM,
        |   LOC.SALE_TM
        |from TEMP_MAC_ALL as TEMP
        |left join TO_YCAK_MAC_D as  MAC  on TEMP.MID = MAC.MID
        |left join TO_YCAK_MAC_LOC_D as LOC on TEMP.MID = LOC.MID
      """.stripMargin).createTempView("TEMP_YCAK_MAC_INFO")

    // 获取机器 套餐名称、投资分成、机器门店、场景信息, 以TEMP_MAC_ALL 表为基准， 对 ycbk 系统对应的ODS层数据 进行统计。
    sparkSession.sql(
      """
        | select
        |    TEMP.MID,
        |    MA.MAC_NM,
        |    MA.PKG_NM,
        |    MA.INV_RATE,
        |    MA.AGE_RATE,
        |    MA.COM_RATE,
        |    MA.PAR_RATE,
        |    MA.IS_ACTV,
        |    MA.ACTV_TM,
        |    MA.HAD_MPAY_FUNC as PAY_SW,
        |    PRVC.PRVC,
        |    CTY.CTY,
        |    AREA.AREA,
        |    CONCAT(MA.SCENE_ADDR,MA.GROUND_NM) as ADDR,
        |    STORE.GROUND_NM as STORE_NM,
        |    STORE.TAG_NM,
        |    STORE.SUB_TAG_NM,
        |    STORE.SUB_SCENE_CATGY_NM,
        |    STORE.SUB_SCENE_NM,
        |    STORE.BRND_NM,
        |    STORE.SUB_BRND_NM
        | from TEMP_MAC_ALL as TEMP
        | left join TO_YCBK_MAC_ADMIN_MAP_D as MA on TEMP.MID = MA.MID
        | left join TO_YCBK_PRVC_D as PRVC on MA.SCENE_PRVC_ID = PRVC.PRVC_ID
        | left join TO_YCBK_CITY_D as CTY on MA.SCENE_CTY_ID = CTY.CTY_ID
        | left join TO_YCBK_AREA_D as AREA on MA.SCENE_AREA_ID = AREA.AREA_ID
        | left join TO_YCBK_MAC_STORE_MAP_D as SMA on TEMP.MID = SMA.MID
        | left join TO_YCBK_STORE_D as STORE on SMA.STORE_ID =  STORE.ID
      """.stripMargin).createTempView("TEMP_YCBK_MAC_INFO")

    sparkSession.sql(
      s"""
         | select
         | 	YCAK.MID,
         | 	YCBK.MAC_NM,
         | 	YCAK.SONG_WHSE_VER,
         | 	YCAK.EXEC_VER,
         | 	YCAK.UI_VER,
         | 	YCAK.HARD_ID,
         | 	YCAK.SALE_TM,
         | 	YCAK.REV_TM,
         | 	YCBK.STORE_NM as OPER_NM,
         | 	if (YCAK.PRVC is null,YCBK.PRVC,YCAK.PRVC) as PRVC,
         | 	if (YCAK.CTY is null,YCBK.CTY,YCAK.CTY) as CTY,
         | 	YCBK.AREA,
         | 	if (YCAK.ADDR_FMT is null,YCBK.ADDR,YCAK.ADDR_FMT) as ADDR,
         | 	YCBK.STORE_NM,
         | 	YCBK.TAG_NM as SCENCE_CATGY,
         | 	YCBK.SUB_SCENE_CATGY_NM as SUB_SCENCE_CATGY,
         | 	YCBK.SUB_TAG_NM as SCENE ,
         | 	YCBK.SUB_SCENE_NM as SUB_SCENE,
         | 	YCBK.BRND_NM as BRND,
         | 	YCBK.SUB_BRND_NM as SUB_BRND,
         | 	YCBK.PKG_NM as PRDCT_NM,
         | 	2 as PRDCT_TYP,
         | 	case when YCBK.PKG_NM = '联营版' then '联营'
         | 	     when YCBK.INV_RATE < 100 then '联营'
         | 	     else '卖断' end BUS_MODE,
         | 	YCBK.INV_RATE,
         | 	YCBK.AGE_RATE,
         | 	YCBK.COM_RATE,
         | 	YCBK.PAR_RATE,
         | 	if (YCAK.STS is null ,YCBK.IS_ACTV,YCAK.STS) as IS_ACTV,
         | 	YCBK.ACTV_TM,
         | 	if (YCAK.PAY_SW is null ,YCBK.PAY_SW,YCAK.PAY_SW) as PAY_SW,
         | 	YCBK.STORE_NM as PRTN_NM,
         | 	YCAK.CUR_LOGIN_TM
         |  FROM TEMP_YCAK_MAC_INFO as YCAK
         |  LEFT JOIN  TEMP_YCBK_MAC_INFO as YCBK
         |  ON YCAK.MID = YCBK.MID
         |
      """.stripMargin).createTempView("result")

    sparkSession.table("result").write.format("Hive").mode(SaveMode.Overwrite).saveAsTable("tw_mac_baseinfo_d_20220401")

    /**
     * 将以上结果写入到MySql中提供查询
     */
    val properties  = new Properties()
    properties.setProperty("user",mysqlUser)
    properties.setProperty("password",mysqlPassword)
    properties.setProperty("driver","com.mysql.cj.jdbc.Driver")


    sparkSession.table("result").write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "tw_mac_baseinfo_d_20220401", properties)
  }


}
