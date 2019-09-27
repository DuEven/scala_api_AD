package release.etl.dm

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import release.constant.ReleaseConstant
import release.util.SparkHelper

/**
  *  曝光集市表
  */
object DMReleaseExposure{
  // 日志
  private val logger: Logger = LoggerFactory.getLogger(DMReleaseCustomer.getClass)

  def main(args: Array[String]): Unit = {
    val appName = "dm_release_job"
    val bdp_day_begin = "20190922"
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
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 缓存级别
    val saveMode = SaveMode.Overwrite
    val storageLevel = ReleaseConstant.DEF_STORAGE_LEVEL

    // 获取日志数据
    val exposureColumns = DMReleaseColumnsHelper.selectDWReleaseExposureColumns()
    val customerColumns = DMReleaseColumnsHelper.selectDWReleaseCustomerColumns()

    // 获取当天数据
    val dateCondition =col(s"${ReleaseConstant.DEF_PARTITION}")===lit(bdp_day)

    //todo 读取曝光表
    val exposureReleaseDF = SparkHelper.readTableData(spark,ReleaseConstant.DW_RELEASE_EXPOSURE,exposureColumns)
    //val exposureReleaseDF = SparkHelper.readJoinTableData(spark,ReleaseConstant.DW_RELEASE_CUSTOMER,ReleaseConstant.DW_RELEASE_EXPOSURE,str,exposureColumns)
      .where(dateCondition)
      .persist(storageLevel)

    //todo 读取用户表
    val customerReleaseDF = SparkHelper.readTableData(spark,ReleaseConstant.DW_RELEASE_CUSTOMER,customerColumns)
      .where(dateCondition)
      .persist(storageLevel)

    val exposureJoinCustomerDF: DataFrame = exposureReleaseDF
      .join(customerReleaseDF,exposureReleaseDF("device_num")===customerReleaseDF("device_num"),"left")

    println("DW========================")

    //println("aaaaaaaaaaaaaaaa"+ exposureJoinCustomerDF.count())

    //exposureJoinCustomerDF.show(10,false)


   //todo  1 统计渠道指标
    val exposureSourceGroupColnmus = Seq[Column](
      $"${ReleaseConstant.COL_RELEASE_SOURCES}",
      $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
      $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}")

    // 插入列
    val exposureSourceColumns = DMReleaseColumnsHelper.selectDMCustomerExposureColumns()
    // 按照需求分组，进行聚合
    val exposureSourceDMDF = exposureJoinCustomerDF.groupBy(exposureSourceGroupColnmus:_*)
      .agg(
        countDistinct(exposureReleaseDF(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_EXPOSURE_COUNT}"),
        (countDistinct(exposureReleaseDF(ReleaseConstant.COL_RELEASE_DEVICE_NUM))/
        count(exposureReleaseDF(ReleaseConstant.COL_RELEASE_DEVICE_NUM)))
          .alias(s"${ReleaseConstant.COL_RELEASE_EXPOSURE_RANTS}")
      )
    // 按照条件查询
      .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
      // 所有维度列
      .selectExpr(exposureSourceColumns:_*)
    // 打印
    println("DM_Source=============================")
    exposureSourceDMDF.show(100,false)
    // 写入hive
    SparkHelper.writetableData(exposureSourceDMDF,ReleaseConstant.DM_RELEASE_EXPOSURE_SOURCE,saveMode)




    // todo 2目标客户多维度分析统计
    val exposureGroupColnmus = Seq[Column](
      $"${ReleaseConstant.COL_RELEASE_SOURCES}",
      $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
      $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
      $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}",
      $"${ReleaseConstant.COL_RELEASE_GENDER}",
      $"${ReleaseConstant.COL_RELEASE_AREA_CODE}"
    )
    // 插入列
    val exposureCubeColumns = DMReleaseColumnsHelper.selectDMExposureCudeColumns()
    // 统计聚合
    val exposureCubeDF = exposureJoinCustomerDF.groupBy(exposureGroupColnmus: _* )
      .agg(
        countDistinct(exposureReleaseDF(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_EXPOSURE_COUNT}"),
        (countDistinct(exposureReleaseDF(ReleaseConstant.COL_RELEASE_DEVICE_NUM))/
          count(exposureReleaseDF(ReleaseConstant.COL_RELEASE_DEVICE_NUM)))
          .alias(s"${ReleaseConstant.COL_RELEASE_EXPOSURE_RANTS}")
      )
      // 按照条件查询
      .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
      // 所有维度列
      .selectExpr(exposureCubeColumns:_*)
    exposureCubeDF.show(100,false)
      // 存入Hive
      SparkHelper.writetableData(exposureCubeDF,ReleaseConstant.DM_RELEASE_EXPOSURE_CUBE,saveMode)



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
