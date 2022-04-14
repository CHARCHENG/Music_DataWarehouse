package dhu.Charlie.utills

import java.text.SimpleDateFormat
import java.math.BigDecimal
import java.util.{Calendar, Date}

object DateUtils {

    /**
    * 将日期转化为string格式
    * @param stringData 输入的日期
    * @return
    */
    def formatDate(stringDate: String):String = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      var dataRes = ""
      try {
        dataRes = sdf.format(sdf.parse(stringDate))
      }catch {
        case e: Exception =>{
          try{
            val bigDecimal = new BigDecimal(stringDate)
            val date = new Date(bigDecimal.longValue())
            dataRes = sdf.format(date)
            // 还有可能出现空值的情况
          }catch{
            case e:Exception=>{
              dataRes

            }
          }
        }
      }
      dataRes
    }

  /**
   * 根据日期进行减法
   * @param dataTime 输入的日期
   * @param day 要减的天数
   * @return
   */
    def dateAdd(dataTime: String, day: Int): String ={
      val sdf = new SimpleDateFormat("yyyyMMdd")
      val date: Date = sdf.parse(dataTime)
      val calendar = Calendar.getInstance()
      calendar.setTime(date)
      calendar.add(Calendar.DATE, -day)
      val per7Date = calendar.getTime
      sdf.format(per7Date)
    }

}
