package release.etl.dm

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import release.constant.ReleaseConstant
import release.util.SparkHelper

/**
  *  曝光集市表
  */
object DMReleaseClick{
  // 日志
  private val logger: Logger = LoggerFactory.getLogger(DMReleaseClick.getClass)

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


    //1
    // 获取日志数据
    val clickColumns = DMReleaseColumnsHelper.selectDWReleaseClickColumns()
    val customerColumns = DMReleaseColumnsHelper.selectDWReleaseCustomerColumns2()



    //2
    // 获取当天数据
    val dateCondition =col(s"${ReleaseConstant.DEF_PARTITION}")===lit(bdp_day)


    //3
    //todo 读取点击表
    val clickReleaseDF = SparkHelper.readTableData(spark,ReleaseConstant.DW_RELEASE_CLICK,clickColumns)
    //val exposureReleaseDF = SparkHelper.readJoinTableData(spark,ReleaseConstant.DW_RELEASE_CUSTOMER,ReleaseConstant.DW_RELEASE_EXPOSURE,str,exposureColumns)
      .where(dateCondition)
      .persist(storageLevel)
    //todo 读取用户表
    val customerReleaseDF = SparkHelper.readTableData(spark,ReleaseConstant.DW_RELEASE_CUSTOMER,customerColumns)
      .where(dateCondition)
      .persist(storageLevel)

    //4
    val clickJoinCustomerDF: DataFrame = clickReleaseDF
      .join(customerReleaseDF,clickReleaseDF("device_num")===customerReleaseDF("device_num"),"left")

    println("DW========================")

    //todo  以上完成表的 join操作 形成一个宽表
    clickJoinCustomerDF.show(10,false)





   //todo  1 统计渠道指标
    val clickGroupColnmus = Seq[Column](
      $"${ReleaseConstant.COL_RELEASE_SOURCES}",
      $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
      $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}",
      $"${ReleaseConstant.COL_RELEASE_AID}"
    )

    // 插入列
    val clickSourceColumns = DMReleaseColumnsHelper.selectDMClickColumns()
    // 按照需求分组，进行聚合
    val clickSourceDMDF = clickJoinCustomerDF.groupBy(clickGroupColnmus:_*)
      .agg(
        count(customerReleaseDF(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_CLICK_COUNT}")
      )
    // 按照条件查询
      .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
      // 所有维度列
      .selectExpr(clickSourceColumns:_*)
    // 打印
    println("DM_Click=============================")
    clickSourceDMDF.show(100,false)
    // 写入hive
    SparkHelper.writetableData(clickSourceDMDF,ReleaseConstant.DM_RELEASE_CLICK_CUBE,saveMode)




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
