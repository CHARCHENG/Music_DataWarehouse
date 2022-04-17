package dhu.Charlie.ods.users

import dhu.Charlie.ods.machine.GenericMacBaseInfo.{mysqlPassword, mysqlUser}
import dhu.Charlie.utills.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Properties

object GenericUsersInfo {
  val hiveMetaUris = ConfigUtils.HIVE_METASTORE_URIS
  val hiveDataBase = ConfigUtils.HIVE_DATABASE

  def main(args: Array[String]): Unit = {
    // 设置操作的用户名
    System.setProperty("HADOOP_USER_NAME", "charlie")
    val sparkSession = SparkSession.builder().master("local[*]").config("hive.metastore.uris", hiveMetaUris).enableHiveSupport().getOrCreate()
    sparkSession.sql(s"use $hiveDataBase")

    //获取支付宝用户全量信息，并注册对应的 TO_YCAK_USR_ALI_D 视图
    val usrAli = sparkSession.sql(
      """
        | SELECT
        |  UID,
        |  REG_MID,
        |  "2" AS REG_CHNL,
        |  openid AS REF_UID,
        |  GDR,
        |  BIRTHDAY,
        |  MSISDN,
        |  LOC_ID,
        |  LOG_MDE,
        |  substring(REG_TM,1,8) AS REG_DT,
        |  substring(REG_TM,9,6) AS REG_TM,
        |  USR_EXP,
        |  SCORE,
        |  user_level,
        |  NVL(user_type,"2") AS USR_TYPE,
        |  is_certified as IS_CERT ,
        |  is_student_certified as IS_STDNT
        |FROM to_users_ali_d
      """.stripMargin)

    //获取QQ 用户全量信息 ，并注册对应的 TO_YCAK_USR_QQ_D 视图
    val usrQQ = sparkSession.sql(
      """
        |SELECT
        | UID,
        | REG_MID,   --机器ID
        | "3" AS REG_CHNL,  -- 1-微信渠道，2-支付宝渠道，3-QQ渠道，4-APP渠道
        | QQID AS REF_UID,
        | GDR,
        | BIRTHDAY,
        | MSISDN,            --手机号码
        | LOC_ID,
        | LOG_MDE,
        | substring(REG_TM,1,8) AS REG_DT,
        | substring(REG_TM,9,6) AS REG_TM,
        | USR_EXP,
        | SCORE,
        | LEVEL,
        | "2" AS USR_TYPE,   --用户类型 1-企业 2-个人
        | NULL AS IS_CERT,   --实名认证
        | NULL AS IS_STDNT   --是否是学生
        |FROM to_users_qq_d
      """.stripMargin)

    //获取APP用户全量信息，并注册对应的 TO_YCAK_USR_APP_D 视图
    val usrApp = sparkSession.sql(
      """
        |SELECT
        | UID,
        | REG_MID,
        | "4" AS REG_CHNL,  -- 1-微信渠道，2-支付宝渠道，3-QQ渠道，4-APP渠道
        | APP_ID AS REF_UID,
        | GDR,
        | BIRTHDAY,
        | MSISDN,
        | LOC_ID,
        | NULL AS LOG_MDE,
        | substring(REG_TM,1,8) AS REG_DT,   --注册日期
        | substring(REG_TM,9,6) AS REG_TM,   --注册时间
        | USR_EXP,
        | 0 AS SCORE,
        | LEVEL,
        | "2" AS USR_TYPE,   --用户类型 1-企业 2-个人
        | NULL AS IS_CERT,   --实名认证
        | NULL AS IS_STDNT   --是否是学生
        |FROM to_users_app_d
      """.stripMargin)

    //获取微信用户全量信息，并注册对应的 TO_YCAK_USR_ALI_D 视图
    var uscWechat = sparkSession.sql(
      """
        | SELECT
        |  UID,
        |  REG_MID,
        |  "1" AS REG_CHNL,  -- 1-微信渠道，2-支付宝渠道，3-QQ渠道，4-APP渠道
        |  wxid AS REF_UID,
        |  GDR,
        |  BIRTHDAY,
        |  MSISDN,
        |  LOC_ID,
        |  LOG_MDE,
        |  substring(REG_TM,1,8) AS REG_DT,
        |  substring(REG_TM,9,6) AS REG_TM,
        |  USR_EXP,
        |  SCORE,
        |  user_level,
        |  "2" AS USR_TYPE,
        |  NULL AS IS_CERT,   --实名认证
        |  NULL AS IS_STDNT   --是否是学生
        |FROM to_users_wechat_d
      """.stripMargin)

    // 将所有信息汇总为一张表
    val allusrInfo = uscWechat.union(usrAli).union(usrQQ).union(usrApp)

      // 获取所有用户的登录信息
    sparkSession.table("to_users_login_d").select("uid").distinct()
      .join(allusrInfo, Seq("uid"), "left").createTempView("TEMP_USR_ACTV")

    // 将结果先保存到EDS层
    sparkSession.table("TEMP_USR_ACTV").write.format("Hive").mode(SaveMode.Overwrite).saveAsTable("TW_USER_BASEINFO_D")


//    /**
//     * 将以上结果写入到MySql中提供查询
//     */
//    val properties  = new Properties()
//    properties.setProperty("user",mysqlUser)
//    properties.setProperty("password",mysqlPassword)
//    properties.setProperty("driver","com.mysql.cj.jdbc.Driver")

  }

}
