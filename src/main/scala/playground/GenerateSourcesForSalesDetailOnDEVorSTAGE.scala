package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, date_format, date_sub, lit}
import org.apache.spark.sql.types.{StringType, StructType}

object GenerateSourcesForSalesDetailOnDEVorSTAGE extends App{

  val spark = SparkSession.builder()
    .appName("Source generation for SaSalesDetail")
    .config("spark.master", "local")
    .config("spark.sql.codegen.wholeStage", "false")
    .getOrCreate()

  //generateDailySalesSources
  generateSalesHistSources
  //generateDailySalesSourcesForPostUpdate


  private def generateDailySalesSourcesForPostUpdate: Unit = {


    val ddlSchemaStr = "`DT` STRING, `FILE_NAME` STRING, `IS_POST` INT, `BUSINESS_DATE` DATE, `LOCATION_NUMBER` BIGINT, `SKU_NUMBER` BIGINT, `TRANSACTION_LINE_NUMBER` BIGINT, `TRANSACTION_NO` BIGINT, `TERMINAL_ID` BIGINT, `TRANSACTION_TYPE` STRING, `GL_POST_DATE` TIMESTAMP, `TRANSACTION_DATE` TIMESTAMP, `TRANSACTION_TIME` STRING, `GMT_OFFSET` DOUBLE, `EXTENDED_DISCOUNT_LOCAL` DOUBLE, `EXTENDED_RETAIL_AMOUNT_LOCAL` DOUBLE, `SALES_TAX` DOUBLE, `TAX_GST` DOUBLE, `TAX_PST` DOUBLE, `TAX_HST` DOUBLE, `TAX_PIF` DOUBLE, `SALES_UNITS` STRING, `DOTCOM_SHIPPING_GROUP_NUMBER` STRING, `ATG_PROFILE_NUMBER` STRING, `BI_CUSTOMER_ID` BIGINT, `EMPLOYEE_ID` STRING, `EMPLOYEE_FIRST_NAME` STRING, `EMPLOYEE_MIDDLE_NAME` STRING, `EMPLOYEE_LAST_NAME` STRING, `SHIPPING_ZIP_CODE` STRING, `EMAIL_RECEIPT_FLAG` STRING, `CASHIER_ID` BIGINT, `RECORD_CREATE_TIME` TIMESTAMP, `ORIGINAL_ATG_SALE_ORDER_NUMBER` STRING, `ATG_ORDER_NUMBER` STRING, `ATG_RETURN_NUMBER` STRING, `RETURN_BUSINESS_DATE` TIMESTAMP, `RETURN_LOCATION_NUMBER` BIGINT, `RETURN_SKU_NUMBER` BIGINT, `RETURN_TRANSACTION_LINE_NUMBER` BIGINT, `RETURN_TRANSACTION_NUMBER` BIGINT, `RETURN_TERMINAL_ID` BIGINT, `PRICE_OVERRIDE_IND` STRING, `ORIGINAL_PRICE` DOUBLE, `TIPS_AMOUNT` DOUBLE, `ARTIST_ID` BIGINT, `ARTIST_FIRST_NAME` STRING, `ARTIST_LAST_NAME` STRING, `ORIGIN_OF_ORDER` STRING, `REASON_CODE` STRING, `DESCRIPTION` STRING, `MPLUS_LOCATION_NUMBER` STRING, `PRODUCT_TAX_CLASS_CODE` STRING, `ORIGIN_TAX_AREA_ID` STRING, `TAX_EXEMPT_IND` STRING, `TAX_OVERRIDE` STRING, `TAX_EXEMPT_PAYEE_NAME` STRING, `TAX_EXEMPT_PAYEE_NO` STRING, `TAX_EXEMPT_PAYEE_ORG` STRING, `RETURN_TAX_AREA_ID` STRING, `ESTIMATED_TAX` STRING, `ACTUAL_TAX_PERCENT` STRING"

    val ddlSchema = StructType.fromDDL(ddlSchemaStr)

    val dailySalesCSV = spark
      .read
      .format("csv")
      .option("header", "true")
      .schema(ddlSchema)
      .load("src/main/resources/data/sa_salesDetail_sources/daily_sales")

    val result = dailySalesCSV.limit(10)
      .withColumn("DT",date_format(date_sub(current_timestamp(),1),"yyyy-MM-dd-HHmmss"))
      //.withColumn("DT", date_format(current_timestamp(),"yyyy-MM-dd-HHmmss").cast(StringType))
      .withColumn("is_post",lit(1))
      .withColumn("Extended_Retail_Amount_Local", lit(13999d).cast(StringType))

    result
      .write
      .mode("overwrite")
      .format("delta")
      .save(s"src/main/resources/data/sa_salesDetail_sources/daily_sales_post")
  }

  private def generateDailySalesSources: Unit = {

    val ddlSchemaStr = "`DT` STRING, `FILE_NAME` STRING, `IS_POST` INT, `BUSINESS_DATE` DATE, `LOCATION_NUMBER` BIGINT, `SKU_NUMBER` BIGINT, `TRANSACTION_LINE_NUMBER` BIGINT, `TRANSACTION_NO` BIGINT, `TERMINAL_ID` BIGINT, `TRANSACTION_TYPE` STRING, `GL_POST_DATE` TIMESTAMP, `TRANSACTION_DATE` TIMESTAMP, `TRANSACTION_TIME` STRING, `GMT_OFFSET` DOUBLE, `EXTENDED_DISCOUNT_LOCAL` DOUBLE, `EXTENDED_RETAIL_AMOUNT_LOCAL` DOUBLE, `SALES_TAX` DOUBLE, `TAX_GST` DOUBLE, `TAX_PST` DOUBLE, `TAX_HST` DOUBLE, `TAX_PIF` DOUBLE, `SALES_UNITS` STRING, `DOTCOM_SHIPPING_GROUP_NUMBER` STRING, `ATG_PROFILE_NUMBER` STRING, `BI_CUSTOMER_ID` BIGINT, `EMPLOYEE_ID` STRING, `EMPLOYEE_FIRST_NAME` STRING, `EMPLOYEE_MIDDLE_NAME` STRING, `EMPLOYEE_LAST_NAME` STRING, `SHIPPING_ZIP_CODE` STRING, `EMAIL_RECEIPT_FLAG` STRING, `CASHIER_ID` BIGINT, `RECORD_CREATE_TIME` TIMESTAMP, `ORIGINAL_ATG_SALE_ORDER_NUMBER` STRING, `ATG_ORDER_NUMBER` STRING, `ATG_RETURN_NUMBER` STRING, `RETURN_BUSINESS_DATE` TIMESTAMP, `RETURN_LOCATION_NUMBER` BIGINT, `RETURN_SKU_NUMBER` BIGINT, `RETURN_TRANSACTION_LINE_NUMBER` BIGINT, `RETURN_TRANSACTION_NUMBER` BIGINT, `RETURN_TERMINAL_ID` BIGINT, `PRICE_OVERRIDE_IND` STRING, `ORIGINAL_PRICE` DOUBLE, `TIPS_AMOUNT` DOUBLE, `ARTIST_ID` BIGINT, `ARTIST_FIRST_NAME` STRING, `ARTIST_LAST_NAME` STRING, `ORIGIN_OF_ORDER` STRING, `REASON_CODE` STRING, `DESCRIPTION` STRING, `MPLUS_LOCATION_NUMBER` STRING, `PRODUCT_TAX_CLASS_CODE` STRING, `ORIGIN_TAX_AREA_ID` STRING, `TAX_EXEMPT_IND` STRING, `TAX_OVERRIDE` STRING, `TAX_EXEMPT_PAYEE_NAME` STRING, `TAX_EXEMPT_PAYEE_NO` STRING, `TAX_EXEMPT_PAYEE_ORG` STRING, `RETURN_TAX_AREA_ID` STRING, `ESTIMATED_TAX` STRING, `ACTUAL_TAX_PERCENT` STRING"

    val ddlSchema = StructType.fromDDL(ddlSchemaStr)

    val dailySalesCSV = spark
      .read
      .format("csv")
      .option("header", "true")
      .schema(ddlSchema)
      .load("src/main/resources/data/sa_salesDetail_sources/daily_sales")

    dailySalesCSV.printSchema()

    println(s"dailySalesCSV.count: ${dailySalesCSV.count}")

    val result = dailySalesCSV
     // .withColumn("DT",date_format(date_sub(current_timestamp(),1),"yyyy-MM-dd-HHmmss")
      .withColumn("DT",date_format(current_timestamp(),"yyyy-MM-dd-HHmmss")
        .cast(StringType))

    result.write.mode("overwrite").format("delta").save(s"src/main/resources/data/sa_salesDetail_sources/daily_sales_converted")
  }


  private def generateSalesHistSources: Unit = {
    val ddlSchemaStr = "`SDCO` DECIMAL(3,0), `SDAPPL` STRING, `SDTYPE` STRING, `SDSTR1` DECIMAL(4,0), `SDSKU1` DECIMAL(9,0), `SDDAT1` DECIMAL(6,0), `SDTM1` DECIMAL(4,0), `SDQTY` DECIMAL(5,0), `SDRET` DECIMAL(9,2), `SDDSCT` DECIMAL(2,0), `SDDSCD` DECIMAL(9,2), `SDTAX` DECIMAL(9,2), `SDTERM` DECIMAL(5,0), `SDCAID` DECIMAL(5,0), `SDTRNO` DECIMAL(10,0), `SDSKU2` DECIMAL(9,0), `SDDTE2` DECIMAL(6,0), `SDCST` DECIMAL(9,2), `SDDST2` DECIMAL(2,0), `SDDSD2` DECIMAL(9,2), `SDDST3` DECIMAL(2,0), `SDDSD3` DECIMAL(9,2), `SDLINE` DECIMAL(3,0), `SDCNTL` DECIMAL(5,0), `SDCRNO` STRING, `CREATE_DATE` TIMESTAMP, `SDDVD2` STRING, `SDPAX` DECIMAL(10,0), `SDTAN` DECIMAL(9,0), `SDTAG` DECIMAL(9,0), `SDVSSL` STRING, `SDAR` STRING, `SDAR2` STRING, `SDARN2` DECIMAL(5,0), `SDARNO` DECIMAL(5,0), `SDCRAM` DECIMAL(9,2), `SDCRTY` STRING, `SDDTE3` DECIMAL(6,0), `SDDVCD` STRING, `SDEMNO` DECIMAL(5,0), `SDNATN` DECIMAL(2,0), `SDPAXR` DECIMAL(2,0), `SDPORD` STRING, `SDPPRT` STRING, `SDPROM` STRING, `SDRETAL` DECIMAL(9,2), `SDSEQ` DECIMAL(2,0), `SDSQ` DECIMAL(2,0), `SDSTAT` STRING, `SDSTR2` DECIMAL(4,0), `SDSUR` DECIMAL(9,2), `SDTRCD` DECIMAL(2,0), `SDDSCDAL` DECIMAL(9,2), `SDTAXAL` DECIMAL(9,2), `SDCSTAL` DECIMAL(9,2), `SDSURAL` DECIMAL(9,2), `SDSQAL` DECIMAL(2,0), `SDDSD2AL` DECIMAL(9,2), `SDDSD3AL` DECIMAL(9,2), `SDCURAL` STRING, `SDXCHGAL` DECIMAL(10,6)"
    val ddlSchema = StructType.fromDDL(ddlSchemaStr)

    val salesHistCSV = spark
      .read
      .format("csv")
      .option("header", "true")
      .schema(ddlSchema)
      .load("src/main/resources/data/sa_salesDetail_sources/sales_hist")

    salesHistCSV.printSchema()
    println(s"salesHistCSV.count: ${salesHistCSV.count}")

    salesHistCSV
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet(s"src/main/resources/data/sa_salesDetail_sources/sales_hist_converted")
  }

}
