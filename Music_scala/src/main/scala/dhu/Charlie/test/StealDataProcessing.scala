package dhu.Charlie.test

import com.alibaba.fastjson.JSON
import dhu.Charlie.test.bean.{Lines, SingleSpo}
import org.apache.spark.sql.SparkSession

import collection.JavaConverters._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control._

case class Entity(label: String, content: String)

object StealDataProcessing {

  def getRelationAndEntity(value: String): Lines = {
    val nObject = JSON.parseObject(value)
    var text = nObject.getString("text")
    val matchStr = List("现场技术组勘察", "经现场勘查", "现场勘查情况", "勘查号", "①侵入", "侵入方", "作案手段", "窃贼特征", "警情编号", "简勘号")
    var i = 0
    val loop = new Breaks;
    loop.breakable{
      for(j <- 0 until 10){
        val nowStr = matchStr(j)
        i = text.indexOf(nowStr)
        if(i != -1){
          loop.break
        }
      }
    }
    if(i == -1){
      i = text.length - 2
    }
    text = text.substring(0, i)
    val relationArray = JSON.parseArray(nObject.getString("relations"))
    val entitesArray = JSON.parseArray(nObject.getString("entities"))
    val relationSize = relationArray.size()
    val entiteySize = entitesArray.size()
    val maps = collection.mutable.Map[Long, Entity]()
    for(x <- 0 until entiteySize){
        val nowJSONObject = entitesArray.getJSONObject(x)
        val id = nowJSONObject.getString("id")
        val label = nowJSONObject.getString("label")
        val start_offset = nowJSONObject.getString("start_offset")
        val end_offset = nowJSONObject.getString("end_offset")
        val content = text.substring(start_offset.toInt, end_offset.toInt)
        maps(id.toLong) = Entity(label, content)
    }
    val lists = collection.mutable.Buffer[SingleSpo]()
    for(x <- 0 until relationSize){
      val nowJSONObject = relationArray.getJSONObject(x)
      val from_id = nowJSONObject.getString("from_id")
      val subjects = maps(from_id.toLong)
      val to_id = nowJSONObject.getString("to_id")
      val objects = maps(to_id.toLong)
      val relation = nowJSONObject.getString("type")
      val spo = new SingleSpo(relation, objects.label, subjects.label, objects.content, subjects.content)
      lists += spo
    }
    new Lines(text, lists.asJava)
  }

  def getRelationAndEntityWithoutSplit(value: String): Lines = {
    val nObject = JSON.parseObject(value)
    var text = nObject.getString("text")
    var i = 0
    i = text.length - 2
    text = text.substring(0, i)
    val relationArray = JSON.parseArray(nObject.getString("relations"))
    val entitesArray = JSON.parseArray(nObject.getString("entities"))
    val relationSize = relationArray.size()
    val entiteySize = entitesArray.size()
    val maps = collection.mutable.Map[Long, Entity]()
    for(x <- 0 until entiteySize){
      val nowJSONObject = entitesArray.getJSONObject(x)
      val id = nowJSONObject.getString("id")
      val label = nowJSONObject.getString("label")
      val start_offset = nowJSONObject.getString("start_offset")
      val end_offset = nowJSONObject.getString("end_offset")
      val content = text.substring(start_offset.toInt, end_offset.toInt)
      maps(id.toLong) = Entity(label, content)
    }
    val lists = collection.mutable.Buffer[SingleSpo]()
    for(x <- 0 until relationSize){
      val nowJSONObject = relationArray.getJSONObject(x)
      val from_id = nowJSONObject.getString("from_id")
      val subjects = maps(from_id.toLong)
      val to_id = nowJSONObject.getString("to_id")
      val objects = maps(to_id.toLong)
      val relation = nowJSONObject.getString("type")
      val spo = new SingleSpo(relation, objects.label, subjects.label, objects.content, subjects.content)
      lists += spo
    }
    new Lines(text, lists.asJava)
  }

  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local[*]").setAppName("Data Processing")
     val sc = new SparkContext(conf)
     val path = "D:\\myProjects\\Music_DataWarehouse\\Music_scala\\src\\main\\resources\\all.jsonl"

     val value = sc.textFile(path)
     val res = value.map(x => {
        var res: Lines = null
        try{
           res = getRelationAndEntity(x)
        }catch {
         case e : Exception =>
           res = getRelationAndEntityWithoutSplit(x)
        }
        res
       }
     )

     val savePath = "D:\\myProjects\\Music_DataWarehouse\\Music_scala\\src\\main\\resources\\train"
     res.coalesce(1).saveAsTextFile(savePath)

////    =====================================test=======================================================
//    val str = "{\"id\":25641,\"text\":\"2018-02-25 15:36:19，手机号：18930855143，报警人在广富林路 三湘四季花城10号601室 近嘉松路，称：报警人称家中失窃，损失不明，请民警到场处理。民警到场后经了解，报警人张凤芳（女，身份证号：310221196404272823，户籍地\\/居住地：上海市徐汇区虹漕路邹家宅38号，联系电话：18930855143）于2018年1月13日15时许离开位于上海市松江区广富林路1599弄10号601室的家中，当时门窗均上锁， 至2018年2月25日15时30分许，发现家中财物被盗。现场勘查情况：入侵方式：从门进入，现场遗留物：无，翻动部位：客厅、主卧和书房，作案人数：1-2人，离开方式：原路返回，采集情况：脚印。损失情况：三套第五套人民币特殊号码投资珍藏纸钞，价值不详；一套人民币无号纸分币二罗马定位册，价值约为人民币1000元。别的物损待清点。\\t1\",\"entities\":[{\"id\":1880,\"label\":\"报警人\",\"start_offset\":100,\"end_offset\":103},{\"id\":1881,\"label\":\"手机号\",\"start_offset\":24,\"end_offset\":35},{\"id\":1882,\"label\":\"日期\",\"start_offset\":172,\"end_offset\":182},{\"id\":1883,\"label\":\"地点\",\"start_offset\":193,\"end_offset\":212},{\"id\":1884,\"label\":\"被盗物品\",\"start_offset\":341,\"end_offset\":343}],\"relations\":[{\"id\":1508,\"from_id\":1880,\"to_id\":1881,\"type\":\"联系方式\"},{\"id\":1509,\"from_id\":1880,\"to_id\":1882,\"type\":\"被盗日期\"},{\"id\":1510,\"from_id\":1880,\"to_id\":1883,\"type\":\"受害地点\"},{\"id\":1511,\"from_id\":1880,\"to_id\":1884,\"type\":\"被盗\"}]}"
//    println(getRelationAndEntity(str))
  }
}
