package playground

import org.apache.spark.sql.SparkSession

object Doordash extends App{

  val spark = SparkSession.builder()
    .appName("Spark Essentials Playground App A")
    .config("spark.master", "local")
    .getOrCreate()

  val orderTenderDetails = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/order_tender_details_events.csv")

  val order_line_consolidated = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/order_line_consolidated.csv")

  val orderHeaderConsolidated = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/order_header_consolidated.csv")

  val smtProduct = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/smtProduct.csv")

  val shiptTlogfile = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/shipt_tlog.csv")

  val marketSellerHistorical = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/market_seller_order_details.csv")

  // DELTA-DELTA-DELTA-DELTA
  val doordash_daily_order_items = spark
    .read
    .parquet("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/doordash_daily_order_items")

  val instacartMLog = spark
    .read
    .parquet("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/instacart_marqueta")

  val instacartTLog = spark
    .read
    .format("delta")
    .parquet("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/instacart_tlog")


}
