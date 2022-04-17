package dhu.Charlie.eds.user

import dhu.Charlie.utills.ConfigUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object GenericActiveUserToMysql {

    val hiveMetaUris = ConfigUtils.HIVE_METASTORE_URIS
    val hiveDataBase = ConfigUtils.HIVE_DATABASE
    val mysqlUrl = ConfigUtils.MYSQL_URL
    val mysqlUser = ConfigUtils.MYSQL_USER
    val mysqlPassword = ConfigUtils.MYSQL_PASSWORD

    def main(args: Array[String]): Unit = {
      // 设置操作的用户名
      System.setProperty("HADOOP_USER_NAME", "charlie")
      val sparkSession = SparkSession.builder().master("local[*]").config("hive.metastore.uris", hiveMetaUris).enableHiveSupport().getOrCreate()

      val properties  = new Properties()
      properties.setProperty("user",mysqlUser)
      properties.setProperty("password",mysqlPassword)
      properties.setProperty("driver","com.mysql.cj.jdbc.Driver")

      sparkSession.sql(s"use $hiveDataBase")
      sparkSession.sql(
        s"""
           |select
           |uid,
           |case when reg_chnl = '1' then '微信'
           |     when reg_chnl = '2' THEN '支付宝'
           |     when reg_chnl = '3' THEN 'QQ'
           |     when reg_chnl = '4' THEN 'APP'
           |     else '未知' END as reg_chnl,   --注册渠道
           |     REF_UID,
           |case when gdr = '0' then '不明'
           |     when gdr = '1' then '男'
           |     when gdr = '2' then '女'
           |     else '不明' end as gdr,        --性别
           |birthday,
           |msisdn as phone,
           |reg_dt,
           |user_level
           |from tw_user_baseinfo_d
           |""".stripMargin
      ).write.mode(SaveMode.Overwrite).jdbc(mysqlUrl, "tw_users_baseinfo_d", properties)
      
    }
}
