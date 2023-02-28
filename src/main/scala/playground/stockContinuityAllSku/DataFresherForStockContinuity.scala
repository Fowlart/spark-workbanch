package playground.stockContinuityAllSku

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, lit}
import org.apache.spark.sql.types.IntegerType

object DataFresherForStockContinuity extends App{

  val spark = SparkSession.builder()
    .appName("Prepare Fresh Data For Inventory Periodic")
    .config("spark.master", "local")
    .getOrCreate()

  /**<h3 style='color: red'> SMT_PRODUCT </h3>*/
  val smt_product = spark
    .read
    .format("delta")
    .load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/smt_product")

  /**<h3 style='color: red'> E3_RAQ </h3>*/
  val e3_raq = spark
    .read
    .format("delta")
    .load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/e3_raq")
    .limit(1000)

  val e3_raq_FRESH = e3_raq.withColumn("Processing_Date", current_date() - 1)

  /**<h3 style='color: red'> INVENTORY_PERIODIC_DAILY </h3>*/
  //val inventory_periodic_daily = spark.read.format("delta").load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/RESULTS/inventory_periodic_with_flags")

 //val inventory_periodic_daily_FRESH = inventory_periodic_daily.withColumn("business_date", current_date() - 1)

  //CHECKS:
  assert(smt_product
    .filter(col("Department_Number").cast(IntegerType) < 60).count() > 100)

 // assert(inventory_periodic_daily_FRESH.filter(col("Active_Store_Flag") === 1 and col("business_date")===current_date() - 1 and ! col("location_number").isin(800,801).count()>100)

  assert(e3_raq_FRESH.filter(col("processing_date")=== current_date() - 1).count() > 100)

  //WRITE:
  e3_raq_FRESH
    .write
    .format("delta")
    .mode("overwrite")
    .save("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/e3_raq_FRESH")

 // inventory_periodic_daily_FRESH.write.format("delta").mode("overwrite").save("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/inventory_periodic_daily_with_flags_FRESH")
}
