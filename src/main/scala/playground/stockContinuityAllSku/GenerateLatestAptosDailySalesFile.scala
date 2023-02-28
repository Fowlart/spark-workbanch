package playground.stockContinuityAllSku

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, lit}

object GenerateLatestAptosDailySalesFile extends App{

  val spark = SparkSession.builder()
    .appName("GenerateLatestAptosDailySalesFile")
    .config("spark.master", "local")
    .getOrCreate()

  val aptosDailySalesStage = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("delimiter","|")
    .option("inferSchema", "true")
    .load("/Users/artur/Downloads/EDW_Daily_Sales_PRE_20230129.txt")
    .withColumn("SUBSCRIPTION_END_DATE",lit(current_date()))
    .withColumn("SDU_ORDER_TYPE",lit("SDU_ORDER_TYPE_MOCKED"))
    .withColumn("COMMERCE_ITEM_TYPE",lit("COMMERCE_ITEM_TYPE_MOCKED"))

  aptosDailySalesStage
    .repartition(1)
    .write
    .format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .mode("overwrite")
    .save("/Users/artur/Downloads/daily_sales_fresh")


}
