package playground.stockContinuityAllSku

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object TestOldEndNewTables extends App {

  val spark = SparkSession.builder()
    .appName("TestOldEndNewTables")
    .config("spark.master", "local")
    .getOrCreate()

  val inventoryPeriodicByScriptOld = spark
      .read
      .parquet("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/inventory_periodic_daily_by_script_sample")
      .filter(col("sku_number").isin(2028546, 2532497, 980995, 2528784, 2268019, 2446631))

  val inventoryPeriodicNewUpdated = spark
      .read
      .format("delta")
      .load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/RESULTS/inventory_periodic_with_flags")
      .filter(col("sku_number").isin(2028546, 2532497, 980995, 2528784, 2268019, 2446631))

  val joined = inventoryPeriodicNewUpdated.as("r").join(inventoryPeriodicByScriptOld.as("s"),
    col("r.Available_Sales_Units") === col("s.Available_Sales_Units") and
      col("r.Net_Vendor_Cost") === col("s.Net_Vendor_Cost") and
      col("r.Damaged_RTV_Units") === col("s.Damaged_RTV_Units") and
      col("r.Ending_Inventory_Cost") === col("s.Ending_Inventory_Cost") and
      col("r.Ending_Inventory_Retail") === col("s.Ending_Inventory_Retail") and
      col("r.Ending_Inventory_Units") === col("s.Ending_Inventory_Units") and
      col("r.Reserve_Units_Picking") === col("s.Reserve_Units_Picking") and
      col("r.Sell_Unit_Cost") === col("s.Sell_Unit_Cost") and
      col("r.Sell_Unit_Retail") === col("s.Sell_Unit_Retail") and

      col("r.Sku_Number") === col("s.Sku_Number") and
      col("r.Location_Number") === col("s.Location_Number"))

  println(s"joined.count: ${joined.count}")
  println(s"inventoryPeriodicByScriptOld.count: ${inventoryPeriodicByScriptOld.count}")
  println(s"inventoryPeriodicNewUpdated.count: ${inventoryPeriodicNewUpdated.count}")

  val resultTable = inventoryPeriodicNewUpdated
    .select(
      col("active_store_flag"),
      col("store_with_inv_flag"),
      col("store_with_sales_flag"),
      col("*"))
    .filter(
        col("active_store_flag")=!=0 ||
        col("store_with_inv_flag")=!=0 ||
        col("store_with_sales_flag")=!=0)

  //resultTable.show()
}