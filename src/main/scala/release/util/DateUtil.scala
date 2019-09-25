package release.util

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
  * @author 杜逸文
  *         Date:2019/09/25  15:33:27
  * @Version ：1.0
  * @description:数据时间处理工具
  *
  */
object DateUtil {

  def deteFormat4String(date:String,fromater:String = "yyyyMMdd"):String={
    if(null == date){
      return null
    }
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(fromater)
    val datetime: LocalDate = LocalDate.parse(date,formatter)

    //校验时间
    datetime.format(DateTimeFormatter.ofPattern(fromater))
  }




  def dateFromat4StringDiff(date:String,diff:Long,fromater:String = "yyyyMMdd"):String = {
    if(null == date){
      return null
    }
    val formatter = DateTimeFormatter.ofPattern(fromater)
    val datetime = LocalDate.parse(date,formatter)
    // 处理天的累加
    val resultDateTime = datetime.plusDays(diff)

    resultDateTime.format(DateTimeFormatter.ofPattern(fromater))
  }



}
