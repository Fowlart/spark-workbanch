package playground

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

object CreatingEmptyDFWithSchema extends App{

  val spark = SparkSession.builder()
    .appName("CreatingEmptyDFWithSchema")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  val ddlSchemaStrSaCustTax = "`transaction_date` TIMESTAMP, `location_number` INT, `business_date` TIMESTAMP, `terminal_id` INT, `transaction_no` INT, `customer_bi_num` BIGINT, `release_no` STRING, `dest_addr_line_1` STRING, `dest_addr_line_2` STRING, `dest_city` STRING, `dest_county` STRING, `dest_state` STRING, `dest_country` STRING, `dest_zip_code` STRING, `dest_tax_area_id` INT, `tax_situs_city_name` STRING, `tax_situs_county` STRING, `tax_situs_state` STRING, `tax_situs_country` STRING, `situs_tax_area_id` STRING, `SA_Shipping_id` STRING, `SA_Situs_id` STRING, `Submitted_Date` STRING, `file_date` DATE, `dt` STRING"
  val ddlSchemaSaCustTax = StructType.fromDDL(ddlSchemaStrSaCustTax)
  val emptySaCustTax = spark.createDataFrame(sc.emptyRDD[Row], ddlSchemaSaCustTax)

  emptySaCustTax.show()

  // SaTaxExempt
  val ddlSchemaStrSaTaxExempt = "`TAX_EXEMPT_PAYEE_NAME` STRING, `TAX_EXEMPT_PAYEE_NO` STRING, `TAX_EXEMPT_PAYEE_ORG` STRING, `tax_exempt_id` STRING, `etl_create_date` TIMESTAMP, `etl_update_date` TIMESTAMP, `RPTING_DT` TIMESTAMP"
  val ddlSchemaSaTaxExempt = StructType.fromDDL(ddlSchemaStrSaTaxExempt)
  val emptySaTaxExempt = spark.createDataFrame(sc.emptyRDD[Row], ddlSchemaSaTaxExempt)

  emptySaTaxExempt.show()
}
