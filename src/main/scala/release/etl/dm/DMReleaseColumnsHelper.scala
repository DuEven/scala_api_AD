package release.etl.dm

import scala.collection.mutable.ArrayBuffer

object DMReleaseColumnsHelper {

  /**
    * 曝光主题
    */
  def selectDWReleaseExposureColumns()={
    val columns = new ArrayBuffer[String]()

    columns.+=("device_num")

    columns
  }

  /**
    * 曝光渠道指标列
    */
  def selectDMCustomerExposureColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("exposure_count")
    columns.+=("exposure_rates")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 曝光多维度分析统计
    */

  def selectDMExposureCudeColumns():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("exposure_count")
    columns.+=("exposure_rates")
    columns.+=("bdp_day")
    columns
  }


  /**
    * 目标客户
    */
  def selectDWReleaseCustomerColumns():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("idcard")
    columns.+=("age")
    columns.+=("getAgeRange(age) as age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 目标客户渠道指标列
    */
  def selectDMCustomerSourceColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("user_count")
    columns.+=("total_count")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 目标客户多维度分析统计
    */
  def selectDMCustomerCudeColumns():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("user_count")
    columns.+=("total_count")
    columns.+=("bdp_day")
    columns
  }
}
