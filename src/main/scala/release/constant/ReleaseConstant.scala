package release.constant

import org.apache.spark.storage.StorageLevel

/**
  * @author 杜逸文
  *         Date:2019/09/25  10:41:15
  * @Version ：1.0
  * @description:常量
  *              常量
  *
  */
object ReleaseConstant {

  //partition      存储内除   执行内存
  val SEF_STORAGE_LEVEL: StorageLevel = StorageLevel.MEMORY_AND_DISK
  val DEF_PARTITION:String = "bdp_day"
  val DEF_SOURCE_PARTITION = 4

  //维度列
  val COL_RLEASE_SESSION_STATUS:String = "release_status"

  //ods
  val ODS_RELEASE_SESSION = "ods_release.ods_01_release_session"

  //dw
  //目标用户主题
  val DW_RELEASE_CUSTOMER = "dw_release.dw_release_customer"

  //曝光主题
  val DW_RELEASE_EXPOSURE = "dw_release.dw_release_exposure"

  //注册主题
  val DW_RELEASE_REGISTER_USER = "dw_release.dw_release_register_users"

  //点击主题
  val DW_RELEASE_CLICK = "dw_release.dw_release_click"


}
