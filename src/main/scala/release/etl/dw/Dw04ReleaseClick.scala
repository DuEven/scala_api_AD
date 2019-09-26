package release.etl.dw

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}
import release.constant.ReleaseConstant
import release.enums.ReleaseStatusEnum
import release.util.SparkHelper

/**
  * @author Duyw
  *         Date:2019/09/25  10:28:49
  * @Version ：1.0
  * @description:DW点击主题
  */
object Dw04ReleaseClick {
  //日志处理
  private val logger: Logger = LoggerFactory.getLogger(Dw01ReleaseCustomer.getClass)

  /**
    *  点击主题 04
    * @param spark
    * @param appName
    * @param bdh_day
    */
  def main(args: Array[String]): Unit = {
    val appName = "dw_release_job"
    val bdp_day_begin = "20190920"
    val bdp_day_end = "20190926"
    // 执行Job
    handleJobs(appName,bdp_day_begin,bdp_day_end)
  }

  def handleReleaseJob(spark:SparkSession,appName:String,bdp_day:String)={
    //获取当前时间
    val begin: Long = System.currentTimeMillis()
    try{
      //导入隐式转换
      import org.apache.spark.sql.functions._          //函数内置包,需要手动到

      //设置缓存级别
      //集市的时候可以作缓存，dm    ods  无法做缓存，数据量太大
      val storagelevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode = SaveMode.Overwrite

      //获取日志字段
      val clickColumns = DwReleaseColumsHelper.selectDwReleaseClick


      // 设置条件 当天数据 点击用户：04
      val clickReleaseCondition =
        (col(s"${ReleaseConstant.DEF_PARTITION}")===lit(bdp_day)) and
      col(s"${ReleaseConstant.COL_RLEASE_SESSION_STATUS}")=== lit(ReleaseStatusEnum.CLICK.getCode)



      val clickReleaseDF = SparkHelper.readTableData(spark,ReleaseConstant.ODS_RELEASE_SESSION,clickColumns)
        // 填入条件
        .where(clickReleaseCondition)
        // 重分区
        .repartition(ReleaseConstant.DEF_SOURCE_PARTITION)


      println("DWReleaseDF=====================================")
      clickReleaseDF.show(100,false)

      //注册用户存储
      SparkHelper.writetableData(clickReleaseDF,ReleaseConstant.DW_RELEASE_CLICK,saveMode)
    }catch {
      //错误信息处理
      case ex:Exception =>{
        logger.error(ex.getMessage,ex)
      }
    }finally {
      //任务处理时长
      s"任务处理时长：${appName},bdp_day = ${bdp_day}, ${System.currentTimeMillis() - begin}"
    }
  }


  /**
    * 点击主题
    */
  def handleJobs(appName:String,bdp_day_begin:String,bdp_day_end:String): Unit ={
    var spark:SparkSession =null
    try{
      // 配置Spark参数
      val conf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        .setAppName(appName)
        .setMaster("local[*]")
      // 创建上下文
      spark = SparkHelper.createSpark(conf)
      // 解析参数    求出需要参与计算的天数
      val timeRange = SparkHelper.rangeDates(bdp_day_begin,bdp_day_end)
      // 循环参数
      for(bdp_day <- timeRange){
        val bdp_date = bdp_day.toString
        handleReleaseJob(spark,appName,bdp_date)
      }
    }catch {
      case ex:Exception=>{
        logger.error(ex.getMessage,ex)
      }
    }
  }



}
