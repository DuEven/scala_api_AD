package release.etl.dm

import release.constant.ReleaseConstant
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import release.util.SparkHelper

/**
  * 投放目标客户数据集市
  */
object DMReleaseCustomer{
  // 日志
  private val logger: Logger = LoggerFactory.getLogger(DMReleaseCustomer.getClass)

  def main(args: Array[String]): Unit = {
    val appName = "dm_release_job"
    val bdp_day_begin = "20190920"
    val bdp_day_end = "20190926"
    // 执行Job
    handleJobs(appName,bdp_day_begin,bdp_day_end)
  }



  /**
    * 统计目标客户集市
    */
  def handleReleaseJob(spark:SparkSession,appName:String,bdp_day:String): Unit ={

    val begin = System.currentTimeMillis()
    try{

    // 导入内置函数和隐式转换
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 缓存级别
    val saveMode = SaveMode.Overwrite
    val storageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
    // 获取日志数据
    val customerColumns = DMReleaseColumnsHelper.selectDWReleaseCustomerColumns()
    // 获取当天数据
    val customerCondition =col(s"${ReleaseConstant.DEF_PARTITION}")===lit(bdp_day)
    val customerReleaseDF = SparkHelper.readTableData(spark,ReleaseConstant.DW_RELEASE_CUSTOMER,customerColumns)
      .where(customerCondition)
      .persist(storageLevel)
    println("DW========================")
    customerReleaseDF.show(10,false)



    // 统计渠道指标
    val customerSourceGroupColnmus = Seq[Column](
      $"${ReleaseConstant.COL_RELEASE_SOURCES}",
      $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
      $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}")

    // 插入列
    val customerSourceColumns = DMReleaseColumnsHelper.selectDMCustomerSourceColumns()
    // 按照需求分组，进行聚合
    val customerSourceDMDF = customerReleaseDF.groupBy(customerSourceGroupColnmus:_*)
      .agg(
        countDistinct(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_USER_COUNT}"),
        count(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}")
      )
    // 按照条件查询
      .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
      // 所有维度列
      .selectExpr(customerSourceColumns:_*)
    // 打印
    println("DM_Source=============================")
    customerSourceDMDF.show(100,false)
    // 写入hive
    SparkHelper.writetableData(customerSourceDMDF,ReleaseConstant.DM_RELEASE_CUSTOMER_SOURCE,saveMode)




    // 目标客户多维度分析统计
    val customerGroupColumns = Seq[Column](
      $"${ReleaseConstant.COL_RELEASE_SOURCES}",
      $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
      $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
      $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}",
      $"${ReleaseConstant.COL_RELEASE_GENDER}",
      $"${ReleaseConstant.COL_RELEASE_AREA_CODE}"
    )
    // 插入列
    val customerCubeColumns = DMReleaseColumnsHelper.selectDMCustomerCudeColumns()
    // 统计聚合
    val customerCubeDF = customerReleaseDF
      .groupBy(customerGroupColumns:_*)
      .agg(
        countDistinct(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_USER_COUNT}"),
        count(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}")
      )
      // 按照条件查询
      .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
      // 所有维度列
      .selectExpr(customerCubeColumns:_*)

      customerCubeDF.show(100,false)
      // 存入Hive
      SparkHelper.writetableData(customerCubeDF,ReleaseConstant.DM_RELEASE_CUSTOMER_CUBE,saveMode)
   }catch {
      case ex:Exception=>{
        logger.error(ex.getMessage,ex)
      }
    }finally {
      println(s"任务处理时长：${appName},bdp_day = ${bdp_day}, ${System.currentTimeMillis() - begin}")
    }
  }




  /**
    * 投放目标用户
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
      // 解析参数
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
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }



}
