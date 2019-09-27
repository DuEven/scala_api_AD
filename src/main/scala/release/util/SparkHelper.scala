package release.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import release.etl.udf.QFUdf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author 杜逸文
  *         Date:2019/09/25  11:22:18
  * @Version ：1.0
  * @description:spark工具类
  */
object SparkHelper {
  /**
    * 读取数据
    */
  private val logger: Logger = LoggerFactory.getLogger(SparkHelper.getClass)

/*  def readJoinTableData(spark:SparkSession,tableName1:String,table2Name:String,same:List[String],colNames:mutable.Seq[String]):DataFrame={
    import spark.implicits._
    //获取数据
    val tableDF1: DataFrame = spark.read.table(tableName1)
      .selectExpr(colNames:_*)
    val tableDF2: DataFrame = spark.read.table(tableName1)
      .selectExpr(colNames:_*)
    val tableDF: DataFrame = tableDF1.join(tableDF2,tableDF1(same(0)) === tableDF2(same(0)))
    tableDF
  }*/

  def readTableData(spark:SparkSession,tableName:String,colNames:mutable.Seq[String]):DataFrame={
    import spark.implicits._
    //获取数据
    val tableDF: DataFrame = spark.read.table(tableName)
      .selectExpr(colNames:_*)
    tableDF
  }

  /**
    * 写入数据
    */
  def writetableData(sourceDF:DataFrame,table:String,mode:SaveMode): Unit ={
    // 写入表
    sourceDF.write.mode(mode).insertInto(table)
  }

  /**
    * 创建SparkSession
    *
    */
  def createSpark(conf:SparkConf):SparkSession={
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    // 加载自定义函数
    regissterFun(spark)

    spark
  }

  /**
    * udf 注册
    */
  def regissterFun(spark:SparkSession)={
    //处理年龄段
    spark.udf.register("getAgeRange",QFUdf.getAgeRange _ )
  }


  /**
    * 参数校验
    *
    */
  def rangeDates(begin:String,end:String):Seq[String]={
    val bdp_days = new ArrayBuffer[String]()
    try{
      val bdp_date_begin= DateUtil.deteFormat4String(begin,"yyyyMMdd")
      val bdp_date_end = DateUtil.deteFormat4String(end,"yyyyMMdd")
      // 如果两个时间相等，取其中的第一个开始时间
      // 如果不相等，计算时间差
      if(begin.equals(end)){
        bdp_days.+=(bdp_date_begin)
      }else{
        var cday = bdp_date_begin
        while (cday != bdp_date_end){
          bdp_days.+=(cday)
          // 让初始时间累加，以天为单位
          val pday = DateUtil.dateFromat4StringDiff(cday,1)
          cday = pday
        }
      }
    }catch {
      case ex:Exception=>{
        println("参数不匹配")
        logger.error(ex.getMessage,ex)
      }
    }
    bdp_days
  }

}
