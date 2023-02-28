package playground.stockContinuityAllSku

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, date_sub}

object DataFresherInventoryPeriodic extends App{

  val spark = SparkSession.builder()
    .appName("Data Fresher Inventory Periodic")
    .config("spark.master", "local")
    .getOrCreate()

  val retfl030_skuloc = spark
    .read
    .parquet("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/retfl030_skuloc")

  retfl030_skuloc.withColumn("Business_Date",current_date())
    .union(retfl030_skuloc.limit(1000).withColumn("Business_Date",date_sub(current_date(), 2)))
    .repartition(1)
    .write
    .mode("overwrite")
    .parquet("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/retfl030_skuloc_FRESH")

  //val e3_raq = spark.read.format("delta").load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/e3_raq")

  //val e3_raq_updated = e3_raq.withColumn("processing_date",current_date() - 1)

  //e3_raq_updated.repartition(col("Processing_Date")).write.format("delta").mode("overwrite").save("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/e3_raq_FRESH")

  //val sa_sales_detail = spark.read.format("delta").load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/sa_sales_detail")

  //sa_sales_detail.limit(100000).withColumn("business_date",date_sub(current_date(), 2)).repartition(col("business_date")).write.format("delta").mode("overwrite").save("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/sa_sales_detail_FRESH")

}
