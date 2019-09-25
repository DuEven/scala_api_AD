package release.etl.dw

import scala.collection.mutable.ArrayBuffer

/**
  * @author 杜逸文
  *         Date:2019/09/25  10:46:45
  * @Version ：1.0
  * @description:DW 层 投放业务列表
  */
object DwReleaseColumsHelper {

  /**
    * 点击主题  04
    * @return
    */
  def selectDwReleaseClick = {
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }


  /**
    * 注册用户主题
    *
    * @return
    */
  def selectDwReleaseRegisterUserColumns = {
    val columns = new ArrayBuffer[String]()
    columns.+=("get_json_object(exts,'$.user_register') as user_register")
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }


  /**
    * 曝光用户
    *
    * @return
    */
  def selectDwReleaseExposureColumns:ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }


  /**
    * 目标用户
    *
    */
  def selectDwReleaseCustomerColumns:ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("get_json_object(exts,'$.idcard') as idcard")
    columns.+=("(cast(date_format(now(),'yyyy')as int) - cast(regexp_extract(get_json_object(exts,'$.idcard'),'(\\\\d{6})(\\\\d{4})',2) as int))as age")
    columns.+=("cast(regexp_extract(get_json_object(exts,'$.idcard'),'(\\\\d{16})(\\\\d{1})()',2) as int) % 2 as gender")
    columns.+=("get_json_object(exts,'$.area_code') as area_code")
    columns.+=("get_json_object(exts,'$.longitude') as longitude")
    columns.+=("get_json_object(exts,'$.latitude') as latitude")
    columns.+=("get_json_object(exts,'$.matter_id') as matter_id")
    columns.+=("get_json_object(exts,'$.model_code') as model_code")
    columns.+=("get_json_object(exts,'$.model_version') as model_version")
    columns.+=("get_json_object(exts,'$.aid') as aid")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }
}
