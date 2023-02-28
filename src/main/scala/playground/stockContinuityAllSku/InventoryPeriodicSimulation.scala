package playground.stockContinuityAllSku

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType}
import org.apache.spark.sql.{Column, SparkSession}

object InventoryPeriodicSimulation extends App {

  val spark = SparkSession.builder()
    .appName("InventoryPeriodicSimulation")
    .config("spark.master", "local")
    .getOrCreate()

  val edwlib_whintr = spark
    .read
    .parquet("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/edwlib_whintr")
  val retfl030_skuloc = spark
    .read
    .parquet("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/retfl030_skuloc")
  val retfl030_whctl = spark
    .read
    .parquet("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/retfl030_whctl")
  val retfl030_whdet = spark
    .read
    .parquet("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/retfl030_whdet")

  val retfl030_skurt = spark
    .read
    .parquet("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/retfl030_skurt")

  val location_dc = spark.read
    .format("delta")
    .load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/location_dc")
  val inventory_periodic_daily = spark.read
    .format("delta")
    .load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/inventory_periodic_daily")

  val saSaleDetails = spark
    .read
    .option("header","true")
    .csv("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/sa_sales_detail")

  val e3_raq = spark
    .read
    .format("delta")
    .load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/e3_raq/")


  val w08 = edwlib_whintr
    .filter(col("WZSTAT") === "2" and col("WZTYPE") === "08")
    .groupBy(col("WZCO"), col("WZSTRE"), col("WZSKU"), col("WZTYPE"))
    .agg(col("WZCO"),
      col("WZSTRE"),
      col("WZSKU"),
      col("WZTYPE"),
      sum(coalesce(col("WZSQTY"), lit(0))).as("QTY"))

  val wXX = edwlib_whintr
    .filter(col("WZSTAT") === "2" and col("WZTYPE") =!= "08")
    .groupBy(col("WZCO"), col("WZSTRE"), col("WZSKU"), col("WZTYPE"))
    .agg(col("WZCO"),
      col("WZSTRE"),
      col("WZSKU"),
      col("WZTYPE"),
      sum(coalesce(col("WZSQTY"), lit(0))).as("QTY"))

  val WHCTRL = retfl030_whdet.as("WD")
    .join(retfl030_whctl.as("WC"),
      col("WD.WDCO") === col("WC.WCCO")
        and col("WD.WDSTRE") === col("WC.WCSTRE")
        and col("WD.WDINV") === col("WC.WCINV"), "left")
    .filter(col("WC.WCSTAT") < 5)
    .groupBy(col("WD.WDCO"), col("WD.WDSTRE"), col("WD.WDSKU"))
    .agg(col("WD.WDCO"), col("WD.WDSTRE"), col("WD.WDSKU"), sum(coalesce(col("WD.WDQOSU"), lit(0))).as("QOSU"))

  val VN = retfl030_skuloc.as("SL").join(retfl030_skurt.as("SK"),
    col("SL.SSCO") === col("SK.SRCO") and col("SL.SSSKU") === col("SK.SRSKU"),
    "left").groupBy(col("SL.SSCO"), col("SL.SSSKU"), col("SL.SSSTRE"))
    .agg(col("SL.SSCO"), col("SL.SSSKU"), col("SL.SSSTRE"), sum(coalesce(col("SK.SRCUC"), lit(0))).as("CUC"),
      lit(0).as("DSC1"), lit(0).as("DSC2"), lit(0).as("DSC3"))

  val inventoryPeriodicInitial = retfl030_skuloc.as("s")

    .join(location_dc.as("l_dc"),
      col("s.SSSTRE") === col("l_dc.Location_Number"), "left")

    .join(w08.as("w08"),
      col("w08.WZCO") === col("SSCO")
        and col("w08.WZSTRE") === col("SSSTRE")
        and col("w08.WZSKU") === col("SSSKU"), "left")

    .join(wXX.as("wXX"),
      col("wXX.WZCO") === col("SSCO")
        and col("wXX.WZSTRE") === col("SSSTRE")
        and col("wXX.WZSKU") === col("SSSKU"), "left")

    .join(WHCTRL.as("WHCTRL"),
      col("WHCTRL.WDCO") === col("SSCO")
        and col("WHCTRL.WDSTRE") === col("SSSTRE")
        and col("WHCTRL.WDSKU") === col("SSSKU"), "left")

    .join(VN.as("VN"),
      col("VN.SSCO") === col("S.SSCO")
        and col("VN.SSSKU") === col("S.SSSKU")
        and col("VN.SSSTRE") === col("S.SSSTRE"), "left")

    .join(inventory_periodic_daily.as("iv"),
      col("iv.Business_Date") === date_sub(current_date(), 2)
        and col("iv.Location_Number") === col("s.SSSTRE")
        and col("s.SSSKU") === col("iv.SKU_Number"), "left")

    .select(
      col("S.Business_Date"),
      col("S.SSSTRE").cast(IntegerType).as("Location_Number"),
      col("S.SSSKU").cast(IntegerType).as("Sku_Number"),
      col("S.SSDMG").cast(IntegerType).as("Damaged_RTV_Units"),
      col("S.SSECD").cast(DecimalType(17, 3)).as("Ending_Inventory_Cost"),
      (col("S.SSOHU") * col("S.SSSUR")).cast(DecimalType(17, 3)).as("Ending_Inventory_Retail"),
      col("S.SSOHU").as("Ending_Inventory_Units"),
      coalesce(col("w08.QTY"), lit(0)).cast(IntegerType).as("In_Transit_Quantity"),
      col("S.SSLOST").cast(IntegerType).as("Lost_Found_Units"),
      lit(0).cast(IntegerType).as("On_Order_Units"),
      lit(0).cast(DecimalType(15, 3)).as("On_Order_Cost"),
      lit(0).cast(DecimalType(15, 3)).as("On_Order_Cost_Retail"),
      col("S.SSROHM").cast(IntegerType).as("Reserve_Units_Merchandise"),
      col("S.SSROHP").cast(IntegerType).as("Reserve_Units_Picking"),
      col("S.SSSUR").cast(DecimalType(17, 3)).as("Sell_Unit_Retail"),
      col("VN.CUC").cast(DecimalType(17, 3)).as("Sell_Unit_Cost"),
      coalesce(col("WHCTRL.QOSU"), lit(0)).cast(IntegerType).as("Store_Order_Quantity"),
      coalesce(col("wXX.QTY"), lit(0)).cast(IntegerType).as("Transfer_Quantity"),
      col("iv.Ending_Inventory_Cost").as("Begin_Inventory_Cost"),
      col("iv.Ending_Inventory_Retail").as("Begin_Inventory_Units"),
      current_timestamp().as("CREATE_DATE"),

      // Available_Sales_Units
      when(col("l_dc.Is_Inv") === "Y"
        and col("l_dc.Country") === "US"
        and col("Is_Sales") === "N"
        and col("l_dc.Is_DmgDC") === "N",
        (col("s.SSOHU") - col("s.SSDMG") - col("s.SSLOST") - col("s.SSROHM") - col("s.SSROHP")).cast(IntegerType))
        .otherwise((col("s.SSOHU") - col("s.SSDMG") - col("s.SSLOST")).cast(IntegerType)).as("Available_Sales_Units"),

      // Net_Vendor_Cost
      (coalesce(col("s.SSOHU"), lit(0)) * coalesce(col("VN.CUC"), lit(0)) * (lit(1) - (coalesce(col("VN.DSC1"), lit(0)) + coalesce(col("VN.DSC3"), lit(0))) / 100))
        .as("Net_Vendor_Cost"))

  /** =1-build view=
    *
    * CREATE view [dbo].[VW_ACTIVE_STORE_PRODUCT_LOCATION] as
    *  SELECT
    *  E3.SKU_Number,
    *  CASE WHEN E3.Location_Number = 701 THEN 700
    *  WHEN E3.Location_Number = 801 THEN 800
    *  WHEN E3.Location_Number = 1001 THEN 1000
    *  WHEN E3.Location_Number = 1021 THEN 1020
    *  WHEN E3.Location_Number = 750 THEN 960
    *  WHEN E3.Location_Number = 1050 THEN 1060
    *
    *  ELSE E3.Location_Number END AS Location_Number
    *  FROM E3_RAQ E3
    *  WHERE E3.Buyer_Class IN ('R', 'W', 'M')
    *  and E3.processing_Date=current_date - 1 **/

  val vw_active_store_product_location = e3_raq
    .filter(
      col("buyer_class").isin("R","W","M")
     //and col("processing_date") === ( current_date() - 1) // TODO: uncomment
    )
    .withColumn(
   "location_number",
       when(col("location_number")==="701",lit("700"))
      .when(col("location_number")==="801",lit("800"))
      .when(col("location_number")==="1001",lit("1000"))
      .when(col("location_number")==="1021",lit("1020"))
      .when(col("location_number")==="750",lit("960"))
         .when(col("location_number")==="1050",lit("1060"))
         .otherwise(col("Location_Number")))
    .select(
      col("location_number"),
      col("sku_number"),
      col("processing_date"))

  /** =2 - COMPUTE JOINED DATA FOR UPDATE: =
    *
    *  UPDATE A
    *  SET Active_Store_Flag = 1,
    *  Store_With_Inv_Flag = (CASE
    *  WHEN (Available_Sales_Units >= 2) THEN 1
    *  ELSE 0
    *  END),
    *  Store_With_Sales_Flag = (CASE
    *  WHEN (Available_Sales_Units + In_Transit_Quantity) >= 2 THEN 1
    *  ELSE 0
    *  END)
    *
    *  =Table for update definition using created view:=
    * FROM DBO.INVENTORY_PERIODIC_DAILY A
    *  INNER JOIN DBo.VW_ACTIVE_STORE_PRODUCT_LOCATION B
    *  ON A.Location_SK = b.Location_SK
    *  AND A.Product_SK = B.Product_SK
    *
    *  -- redundant filter, since vw_active_store_product_location should
    *  -- be filtered filtered: E3.processing_Date=current_date - 1
    *  WHERE A.Date_SK = @intDATE_SK
    *
    *  AND EXISTS (SELECT 1 FROM SA_SALES_DETAIL C
    *  WHERE C.LOCATION_SK = B.LOCATION_SK
    *  AND C.TRANSACTION_TYPE = '01'
    *  AND C.PROCESSING_DATE_SK BETWEEN @intSDATE_SK AND @intEDATE_SK
    *  )* */

  val saSalesDetailPrepared = saSaleDetails.select("store_number", "transaction_type", "rpting_dt","transaction_type")
    .filter(col("transaction_type") === "SALE")
    // TODO: uncomment
    //.filter(col("business_date").between(date_sub(current_date(), 7), date_sub(current_date(), 1)))
    .distinct()

  val joinedForFlagsComputing = inventoryPeriodicInitial.as("a").join(vw_active_store_product_location.as("b"),
    col("a.location_number") === col("b.location_number") and
      col("a.sku_number") === col("b.sku_number"))
    .join(saSalesDetailPrepared.as("c"), col("c.store_number") === col("a.location_number"))

    .select(
      col("available_sales_units").as("jffc_available_sales_units"),
      col("in_transit_quantity").as("jffc_in_transit_quantity"),
      col("a.location_number").as("jffc_location_number"),
      col("a.sku_number").as("jffc_sku_number"))

  val inventoryPeriodicDailyWithFlags = inventoryPeriodicInitial.as("input").join(joinedForFlagsComputing,
    col("input.location_number") === col("jffc_location_number") and
      col("input.sku_number") === col("jffc_sku_number"), "left")
    //TODO: check otherwise, make 'isNotNull' more accurate
    .withColumn("active_store_flag", when(col("jffc_location_number").isNotNull, lit(1)).otherwise(lit(0)))
    .withColumn("store_with_inv_flag", when(col("jffc_available_sales_units") >= 2, lit(1)).otherwise(lit(0)))
    .withColumn("store_with_sales_flag", when((col("jffc_available_sales_units") + col("jffc_in_transit_quantity")) >= 2, lit(1)).otherwise(lit(0)))

    .select(
      col("active_store_flag"),
      col("store_with_inv_flag"),
      col("store_with_sales_flag"),
      col("input.*"))


  /**
    * =TEMP_IP_DAILY_ACTIVE_FLAG=
    * SELECT
    *  A.Date_SK,
    *  A.Product_SK,
    *  ldc.DC_Name,
    *  MIN(A.Location_SK) AS Location_SK,
    *  SUM(Available_Sales_Units) AS Available_Sales_Units,
    *  SUM(In_Transit_Quantity) AS In_Transit_Quantity
    *  INTO #TEMP_IP_DAILY_ACTIVE_FLAG
    *  FROM DBO.INVENTORY_PERIODIC_DAILY A (NOLOCK)
    *  INNER JOIN Location_DC ldc on ldc.Location_sk=A.location_sk
    *  WHERE A.Date_SK = @intDATE_SK
    *  AND ldc.Is_Dotcom='Y' and ldc.Is_DmgDC='N' and ldc.DC_Name is not null
    *  GROUP BY A.Date_SK, A.Product_SK, ldc.DC_Name
    *  */

  val tempIpDailyActiveFlag = inventoryPeriodicDailyWithFlags.as("A")
    .join(location_dc.as("ldc"), col("A.location_number") === col("ldc.location_number"))
    .filter(col("ldc.is_dotcom") === "Y")
    //TODO: uncomment
    //.filter(col("A.business_date")=== current_date() - 1)
    //TODO: check if col("A.Date_SK") needed
    .filter(col("ldc.is_dmgdc") === "N")
    .filter(notNullColumn("ldc.dc_name"))
    .groupBy(col("A.business_date"), col("A.sku_number"), col("ldc.dc_name"))
    .agg(
      col("A.business_date"),
      col("A.sku_number"),
      col("ldc.dc_name"),
      min(col("A.location_number")).as("location_number"),
      sum(col("available_sales_units")).as("available_sales_units"),
      sum(col("in_transit_quantity")).as("in_transit_quantity"))

  /** UPDATE A
    *  SET A.Store_With_Inv_Flag = (CASE
    *  WHEN (B.Available_Sales_Units >= 2) THEN 1
    *  ELSE 0
    *  END), --Removed  'OR B.Available_Sales_Units < 0' 1/23/2006 BW
    *  A.Store_With_Sales_Flag = (CASE
    *  WHEN (B.Available_Sales_Units + B.In_Transit_Quantity) >= 2 THEN 1
    *  ELSE 0 END)
    *  FROM DBO.INVENTORY_PERIODIC_DAILY A
    *  INNER JOIN #TEMP_IP_DAILY_ACTIVE_FLAG B
    *  ON A.Date_SK = b.Date_SK
    *  AND A.Location_SK = b.Location_SK
    *  AND A.Product_SK = B.Product_SK **/
  val inventoryPeriodicDailyWithFlagsUpdated = inventoryPeriodicDailyWithFlags.as("A").join(tempIpDailyActiveFlag.as("temp"),

      col("A.business_date") === col("temp.business_date")
      and col("A.location_number") === col("temp.location_number")
      and col("A.sku_number") === col("temp.sku_number"), "left")

    //store_with_inv_flag update
    .withColumn("store_with_inv_flag_a",
      when(notNullColumn("temp.sku_number") and col("temp.available_sales_units") >= 2, lit(1))
        .when(notNullColumn("temp.sku_number") and col("temp.available_sales_units") < 2, lit(0))
        .when(isNullColumn("temp.sku_number"), col("A.store_with_inv_flag")))

    //store_with_sales_flag update
    .withColumn("store_with_sales_flag_a",
      when(notNullColumn("temp.sku_number") and
        (col("temp.available_sales_units") + col("temp.in_transit_quantity")) >= 2, lit(1))
        .when(notNullColumn("temp.sku_number") and
          (col("temp.available_sales_units") + col("temp.in_transit_quantity")) < 2, lit(0))
        .when(isNullColumn("temp.sku_number"), col("A.store_with_sales_flag")))

    .select("A.*","store_with_inv_flag_a","store_with_sales_flag_a")
    .drop("store_with_inv_flag","store_with_sales_flag")
    .withColumnRenamed("store_with_inv_flag_a","store_with_inv_flag")
    .withColumnRenamed("store_with_sales_flag_a","store_with_sales_flag")

    // checks
  inventoryPeriodicDailyWithFlagsUpdated.filter(col("sku_number")
    .isin(2028546, 2532497, 980995, 2528784, 2268019, 2446631))
    .write
    .mode("overwrite")
    .parquet("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/inventory_periodic_daily_updated_sample")

  private def isNullColumn(colName: String): Column = {
    (col(colName).isNull || col(colName).cast(StringType) === "" || col(colName).cast(StringType) === "null")
  }

  private def notNullColumn(colName: String): Column = {
    (col(colName).isNotNull && col(colName).cast(StringType) =!= "" && col(colName).cast(StringType) =!= "null")
  }
}
