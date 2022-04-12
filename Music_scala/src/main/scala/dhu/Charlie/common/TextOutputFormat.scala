package dhu.Charlie.common

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class TextOutputFormat extends MultipleTextOutputFormat[Any, Any]{
  /**
   * 根据key和value自定义输出文件名
   * @return
   */
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    //将引用转换为子类的引用。
    val fileName=key.asInstanceOf[String]
    fileName
  }

  //文件内容：默认同时输出key和value。这里指定不输出key。
  override def generateActualKey(key: Any, value: Any): String = {
    null
  }
}
