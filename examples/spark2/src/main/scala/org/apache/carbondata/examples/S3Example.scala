package org.apache.carbondata.examples

import java.io.File

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object S3Example {
  val logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath
  val warehouse = s"$rootPath/integration/hive/target/warehouse"
  val metastoredbS3 = s"$rootPath/integration/hive/target/s3_metastore_db"
  //  val metastoredb = s"/home/bhavya/sparktest/comparetestsort_metastore_db"
  val metastoredbLocal = s"$rootPath/integration/hive/target/"

  val metastoredb = s"$rootPath/integration/hive/target/"

  val carbonTableName = "comparetest_hive_carbon"

  def main(args: Array[String]) {
    CarbonProperties.getInstance()
      .addProperty("carbon.enable.vector.reader", "true")
      .addProperty("enable.unsafe.sort", "true")
      .addProperty("carbon.blockletgroup.size.in.mb", "32")
      //.addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
      //.addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      // .addProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS)
      .addProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.CARBON_LOCK_TYPE_S3)
      .addProperty(CarbonCommonConstants.S3_ACCESS_KEY, "")
      .addProperty(CarbonCommonConstants.S3_SECRET_KEY, "")
      .addProperty(CarbonCommonConstants.S3_ENDPOINT, "")
      .addProperty(CarbonCommonConstants.S3_SSL_ENABLED, "false")
      .addProperty(CarbonCommonConstants.S3_MAX_ERROR_RETRIES, "2")
      .addProperty(CarbonCommonConstants.S3_MAX_CLIENT_RETRIES, "2")
      .addProperty(CarbonCommonConstants.S3_MAX_CONNECTIONS, "20")
      .addProperty(CarbonCommonConstants.S3_STAGING_DIRECTORY, "carbonData")
      .addProperty(CarbonCommonConstants.S3_USE_INSTANCE_CREDENTIALS, "false")
      .addProperty(CarbonCommonConstants.S3_PIN_CLIENT_TO_CURRENT_REGION, "false")
      .addProperty(CarbonCommonConstants.S3_SSE_ENABLED, "false")

    import org.apache.spark.sql.CarbonSession._

    val carbon: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("CompareTestExample")
      .config("carbon.sql.warehouse.dir", warehouse).enableHiveSupport()
      .getOrCreateCarbonSession(
        "s3a://testcarbon12345", metastoredbS3)

    carbon.sql(s"drop table if exists REGION");
  }
}
