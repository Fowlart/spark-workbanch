package playground.stockContinuityAllSku

import org.apache.spark.sql.{SaveMode, SparkSession}

object WriteDataForTests extends App{

  val spark = SparkSession.builder()
    .appName("write data for tests")
    .config("spark.master", "local")
    .getOrCreate()

  val sa_sales_detail_FRESH =
    spark
    .read
    .format("delta")
    .load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/sa_sales_detail_FRESH")

  sa_sales_detail_FRESH
    .limit(5000)
    .repartition(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet("/Users/artur/IdeaProjects/dp-analytics-2.0/aggregation" +
      "/src/test/resources/feeds/inventory_periodic_daily/sa_sales_details")

  //retfl030_skurt
  val retfl030_skurt = spark
    .read
    .parquet("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/retfl030_skurt")

  retfl030_skurt
    .limit(5000)
    .repartition(1)
    .write
    .mode(SaveMode.Overwrite)
    .save("/Users/artur/IdeaProjects/dp-analytics-2.0/aggregation/src/test/" +
      "resources/feeds/inventory_periodic_daily/retfl030_skurt")

  // retfl030_whdet
  val retfl030_whdet = spark
    .read
    .format("parquet")
    .load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/retfl030_whdet")

  retfl030_whdet
    .limit(5000)
    .repartition(1)
    .write
    .mode(SaveMode.Overwrite)
    .save("/Users/artur/IdeaProjects/dp-analytics-2.0/aggregation/src/test/" +
      "resources/feeds/inventory_periodic_daily/retfl030_whdet")

  // retfl030_whctl
  val retfl030_whctl = spark
    .read
    .format("parquet")
    .load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/retfl030_whctl")

  retfl030_whctl
    .limit(5000)
    .repartition(1)
    .write
    .mode(SaveMode.Overwrite)
    .save("/Users/artur/IdeaProjects/dp-analytics-2.0/aggregation/src/test/" +
      "resources/feeds/inventory_periodic_daily/retfl030_whctl")

  //retfl030_skuloc
  val retfl030_skuloc = spark
    .read
    .format("parquet")
    .load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/retfl030_skuloc")

  retfl030_skuloc
    .limit(5000)
    .repartition(1)
    .write
    .mode(SaveMode.Overwrite)
    .save("/Users/artur/IdeaProjects/dp-analytics-2.0/aggregation/src/test/" +
      "resources/feeds/inventory_periodic_daily/retfl030_skuloc")
  
  //edwlib_whintr
  val edwlib_whintr = spark
    .read
    .format("parquet")
    .load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/edwlib_whintr")

  edwlib_whintr
    .limit(5000)
    .repartition(1)
    .write
    .mode(SaveMode.Overwrite)
    .save("/Users/artur/IdeaProjects/dp-analytics-2.0/aggregation/src/test/" +
      "resources/feeds/inventory_periodic_daily/edwlib_whintr")

}
