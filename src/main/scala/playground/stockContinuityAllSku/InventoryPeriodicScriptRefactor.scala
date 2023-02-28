package playground.stockContinuityAllSku

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}

object InventoryPeriodicScriptRefactor extends App {

  val spark = SparkSession.builder()
    .appName("InventoryPeriodicSimulation")
    .config("spark.master", "local")
    .getOrCreate()

  val edwlib_whintr = spark
    .read
    .parquet("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/edwlib_whintr")
  val retfl030_skuloc = spark
    .read
    .parquet("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/retfl030_skuloc_FRESH")
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
    .format("delta")
    .load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/sa_sales_detail_FRESH")
  val e3_raq = spark
    .read
    .format("delta")
    .load("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/SOURCES/e3_raq_FRESH")

  edwlib_whintr.createTempView("mcs_edwlib_WHINTR")
  retfl030_skuloc.createTempView("mcs_retfl030_skuloc")
  retfl030_whctl.createTempView("mcs_retfl030_whctl")
  retfl030_whdet.createTempView("mcs_retfl030_WHDET")
  retfl030_skurt.createTempView("mcs_retfl030_SKURT")
  location_dc.createTempView("hdp_location_dc")
  inventory_periodic_daily.createTempView("analytics_inventory_periodic_daily")

  val inventory_periodic_daily_by_script = spark.sql(
    """select
      |    s.Business_Date,
      |    cast(s.SSSTRE as Int) as Location_Number,
      |    cast(s.SSSKU  as Int) as Sku_Number,
      |	cast((case when l_dc.Is_Inv ='Y' and l_dc.Country ='US' and Is_Sales ='N' and l_dc.Is_DmgDC='N' Then s.SSOHU - (s.SSDMG + s.SSLOST + s.SSROHM + s.SSROHP) Else s.SSOHU - (s.SSDMG + s.SSLOST) end) as Int) as Available_Sales_Units,
      |	cast(S.SSDMG as Int) as Damaged_RTV_Units,
      |	cast(S.SSECD as decimal(17,3)) as Ending_Inventory_Cost,
      |	cast(S.SSOHU * S.SSSUR as decimal(17,3)) as Ending_Inventory_Retail,
      |	cast(S.SSOHU as Int)  as  Ending_Inventory_Units,
      |	cast(IfNULL(w08.QTY,0) as Int) as In_Transit_Quantity,
      |	cast(S.SSLOST as Int) as Lost_Found_Units,
      |	ifNull(s.SSOHU,0) * ( (1 - (ifNull((VN.DSC1 + VN.DSC3),0)/100)) * IfNull(VN.CUC,0)) as Net_Vendor_Cost,
      |	cast(0 as int) AS On_Order_Units,
      |	cast(0 as decimal(15,3)) as On_Order_Cost,
      |	cast(0 as decimal(15,3)) as On_Order_Cost_Retail,
      |	cast(S.SSROHM as Int)  as Reserve_Units_Merchandise,
      |	cast(S.SSROHP as Int)  as Reserve_Units_Picking,
      |	cast(S.SSSUR as decimal(17,3)) as Sell_Unit_Retail,
      |	cast(VN.CUC as decimal(17,3)) AS Sell_Unit_Cost,
      |	cast(IfNull(WHCTRL.QOSU,0) as Int) as Store_Order_Quantity,
      |	cast(IfNULL(wXX.QTY,0) as Int) AS Transfer_Quantity,
      |    iv.Ending_Inventory_Cost as Begin_Inventory_Cost,
      |    iv.Ending_Inventory_Retail as Begin_Inventory_Retail,
      |    iv.Ending_Inventory_Units as Begin_Inventory_Units,
      |	current_timestamp() as CREATE_DATE
      |
      |from mcs_retfl030_SKULOC s
      |left outer join hdp_location_dc l_dc on s.SSSTRE = l_dc.Location_Number

      |LEFT OUTER join (SELECT WZCO, WZSTRE, WZSKU, WZTYPE, SUM(ifnull(WZSQTY,0)) QTY FROM mcs_edwlib_WHINTR WHERE WZSTAT = '2' and WZTYPE = '08' GROUP BY WZCO, WZSTRE, WZSKU,WZTYPE) AS w08 on w08.WZCO= SSCO AND w08.WZSTRE = SSSTRE AND w08.WZSKU = SSSKU
      |
      |LEFT OUTER join (SELECT WZCO, WZSTRE, WZSKU, WZTYPE, SUM(ifnull(WZSQTY,0)) QTY FROM mcs_edwlib_WHINTR WHERE WZSTAT = '2' and WZTYPE <> '08' GROUP BY WZCO, WZSTRE, WZSKU,WZTYPE) AS wXX on wXX.WZCO= SSCO AND wXX.WZSTRE = SSSTRE AND wXX.WZSKU = SSSKU
      |
      |LEFT OUTER JOIN (SELECT WD.WDCO, WD.WDSTRE, WD.WDSKU, SUM(ifnull(WD.WDQOSU,0)) QOSU FROM mcs_retfl030_WHDET WD LEFT OUTER JOIN mcs_retfl030_WHCTL WC ON WD.WDCO = WC.WCCO AND WD.WDSTRE = WC.WCSTRE AND WD.WDINV = WC.WCINV
      |WHERE WC.WCSTAT < 5 GROUP BY WD.WDCO, WD.WDSTRE, WD.WDSKU) WHCTRL on WHCTRL.WDCO= SSCO AND WHCTRL.WDSTRE = SSSTRE AND WHCTRL.WDSKU = SSSKU
      |
      |LEFT OUTER JOIN (SELECT SL.SSCO, SL.SSSKU, SL.SSSTRE, SUM(ifnull(SK.SRCUC,0)) CUC, 0 as DSC1, 0 as  DSC2, 0 as DSC3 FROM mcs_retfl030_SKULOC SL
      |		LEFT OUTER JOIN mcs_retfl030_SKURT SK ON SL.SSCO = SK.SRCO AND SL.SSSKU = SK.SRSKU
      |		GROUP BY SL.SSCO, SL.SSSKU, SL.SSSTRE) VN ON VN.SSCO = S.SSCO AND VN.SSSKU = S.SSSKU AND VN.SSSTRE = S.SSSTRE
      |LEFT OUTER JOIN analytics_inventory_periodic_daily iv on iv.Business_Date = current_date()-2 and iv.Location_Number = s.SSSTRE and s.SSSKU = iv.SKU_Number""".stripMargin)


  val w08_by_script = spark.sql("SELECT WZCO, WZSTRE, WZSKU, WZTYPE, SUM(ifnull(WZSQTY,0)) QTY " +
    "FROM mcs_edwlib_WHINTR WHERE WZSTAT = '2' and WZTYPE = '08' GROUP BY WZCO, WZSTRE, WZSKU,WZTYPE")

  val w08 = edwlib_whintr
    .filter(col("WZSTAT")==="2" and col("WZTYPE")==="08")
    .groupBy(col("WZCO"),col("WZSTRE"),col("WZSKU"),col("WZTYPE"))
    .agg(col("WZCO"),
      col("WZSTRE"),
      col("WZSKU"),
      col("WZTYPE"),
      sum(coalesce(col("WZSQTY"),lit(0))).as("QTY"))

 //println(s"w08_by_script.count: ${w08_by_script.count}")
 //println(s"w08.count: ${w08.count}")

  val wXX_by_script = spark.sql("SELECT WZCO, WZSTRE, WZSKU, WZTYPE, SUM(ifnull(WZSQTY,0)) QTY " +
    "FROM mcs_edwlib_WHINTR WHERE WZSTAT = '2' and WZTYPE <> '08' GROUP BY WZCO, WZSTRE, WZSKU,WZTYPE")

  val wXX = edwlib_whintr
    .filter(col("WZSTAT") === "2" and col("WZTYPE") =!= "08")
    .groupBy(col("WZCO"), col("WZSTRE"), col("WZSKU"), col("WZTYPE"))
    .agg(col("WZCO"),
      col("WZSTRE"),
      col("WZSKU"),
      col("WZTYPE"),
      sum(coalesce(col("WZSQTY"), lit(0))).as("QTY"))

  //println(s"wXX_by_script.count: ${wXX_by_script.count}")
  //println(s"wXX.count: ${wXX.count}")

  val WHCTRL_by_script = spark.sql("""SELECT WD.WDCO, WD.WDSTRE, WD.WDSKU, SUM(ifnull(WD.WDQOSU,0)) QOSU FROM
                                     |mcs_retfl030_WHDET WD LEFT OUTER JOIN mcs_retfl030_WHCTL WC ON
                                     |WD.WDCO = WC.WCCO AND WD.WDSTRE = WC.WCSTRE AND WD.WDINV = WC.WCINV
                                     |WHERE WC.WCSTAT < 5 GROUP BY WD.WDCO, WD.WDSTRE, WD.WDSKU""".stripMargin)

  val WHCTRL = retfl030_whdet.as("WD")
    .join(retfl030_whctl.as("WC"),
          col("WD.WDCO")===col("WC.WCCO")
      and col("WD.WDSTRE")===col("WC.WCSTRE")
      and col("WD.WDINV")===col("WC.WCINV"),"left")
    .filter(col("WC.WCSTAT")<5)
    .groupBy(col("WD.WDCO"),col("WD.WDSTRE"),col("WD.WDSKU"))
    .agg(col("WD.WDCO"),col("WD.WDSTRE"),col("WD.WDSKU"),sum(coalesce(col("WD.WDQOSU"),lit(0))).as("QOSU"))

  //println(s"WHCTRL_by_script.count: ${WHCTRL_by_script.count}")
  //println(s"WHCTRL.count: ${WHCTRL.count}")

  val VN_by_script = spark.sql("""SELECT SL.SSCO, SL.SSSKU, SL.SSSTRE, SUM(ifnull(SK.SRCUC,0)) CUC, 0 as DSC1, 0 as  DSC2, 0 as DSC3 FROM
                       |mcs_retfl030_SKULOC SL
                       |		LEFT OUTER JOIN mcs_retfl030_SKURT SK ON SL.SSCO = SK.SRCO AND SL.SSSKU = SK.SRSKU
                       |		GROUP BY SL.SSCO, SL.SSSKU, SL.SSSTRE""".stripMargin)


  val VN = retfl030_skuloc.as("SL").join(retfl030_skurt.as("SK"),
    col("SL.SSCO")===col("SK.SRCO") and col("SL.SSSKU")===col("SK.SRSKU"),
    "left").groupBy(col("SL.SSCO"),col("SL.SSSKU"),col("SL.SSSTRE"))
    .agg(col("SL.SSCO"),col("SL.SSSKU"),col("SL.SSSTRE"),sum(coalesce(col("SK.SRCUC"),lit(0))).as("CUC"),
      lit(0).as("DSC1"),lit(0).as("DSC2"),lit(0).as("DSC3"))

  //println(s"VN_by_script.count: ${VN_by_script.count}")
  //println(s"VN.count: ${VN.count}")

  val inventory_periodic_daily_by_dsl = retfl030_skuloc.as("s")

    .join(location_dc.as("l_dc"),
      col("s.SSSTRE")===col("l_dc.Location_Number"),"left")

    .join(w08.as("w08"),
        col("w08.WZCO")===col("SSCO")
          and col("w08.WZSTRE")===col("SSSTRE")
          and col("w08.WZSKU")===col("SSSKU"),"left")

    .join(wXX.as("wXX"),
      col("wXX.WZCO")===col("SSCO")
      and col("wXX.WZSTRE")===col("SSSTRE")
      and col("wXX.WZSKU")===col("SSSKU"),"left")

    .join(WHCTRL.as("WHCTRL"),
        col("WHCTRL.WDCO") === col("SSCO")
        and col("WHCTRL.WDSTRE") === col("SSSTRE")
        and col("WHCTRL.WDSKU") === col("SSSKU"), "left")

    .join(VN.as("VN"),
      col("VN.SSCO") === col("S.SSCO")
      and col("VN.SSSKU") === col("S.SSSKU")
      and col("VN.SSSTRE") === col("S.SSSTRE"), "left")

    .join(inventory_periodic_daily.as("iv"),
      col("iv.Business_Date") === date_sub(current_date(),2)
        and col("iv.Location_Number") === col("s.SSSTRE")
        and col("s.SSSKU") === col("iv.SKU_Number"), "left")

    .select(
      col("S.Business_Date"),
      col("S.SSSTRE").cast(IntegerType).as("Location_Number"),
      col("S.SSSKU").cast(IntegerType).as("Sku_Number"),
      col("S.SSDMG").cast(IntegerType).as("Damaged_RTV_Units"),
      col("S.SSECD").cast(DecimalType(17,3)).as("Ending_Inventory_Cost"),
      (col("S.SSOHU") * col("S.SSSUR")).cast(DecimalType(17,3)).as("Ending_Inventory_Retail"),
      col("S.SSOHU").as("Ending_Inventory_Units"),
      coalesce(col("w08.QTY"),lit(0)).cast(IntegerType).as("In_Transit_Quantity"),
      col("S.SSLOST").cast(IntegerType).as("Lost_Found_Units"),
      lit(0).cast(IntegerType).as("On_Order_Units"),
      lit(0).cast(DecimalType(15,3)).as("On_Order_Cost"),
      lit(0).cast(DecimalType(15,3)).as("On_Order_Cost_Retail"),
      col("S.SSROHM").cast(IntegerType).as("Reserve_Units_Merchandise"),
      col("S.SSROHP").cast(IntegerType).as("Reserve_Units_Picking"),
      col("S.SSSUR").cast(DecimalType(17,3)).as("Sell_Unit_Retail"),
      col("VN.CUC").cast(DecimalType(17,3)).as("Sell_Unit_Cost"),
      coalesce(col("WHCTRL.QOSU"),lit(0)).cast(IntegerType).as("Store_Order_Quantity"),
      coalesce(col("wXX.QTY"),lit(0)).cast(IntegerType).as("Transfer_Quantity"),
      col("iv.Ending_Inventory_Cost").as("Begin_Inventory_Cost"),
      col("iv.Ending_Inventory_Retail").as("Begin_Inventory_Units"),
      current_timestamp().as("CREATE_DATE"),

      // Available_Sales_Units
      when(col("l_dc.Is_Inv")==="Y"
        and col("l_dc.Country")==="US"
          and col("Is_Sales")==="N"
            and col("l_dc.Is_DmgDC")==="N",
        (col("s.SSOHU")-col("s.SSDMG")-col("s.SSLOST")-col("s.SSROHM")-col("s.SSROHP")).cast(IntegerType))
        .otherwise((col("s.SSOHU")-col("s.SSDMG")-col("s.SSLOST")).cast(IntegerType)).as("Available_Sales_Units"),

      // Net_Vendor_Cost
      (coalesce(col("s.SSOHU"),lit(0)) * coalesce(col("VN.CUC"),lit(0)) * (lit(1) - (coalesce(col("VN.DSC1"),lit(0)) + coalesce(col("VN.DSC3"),lit(0)))/100 ))
        .as("Net_Vendor_Cost"))

  inventory_periodic_daily_by_script
       .write
       .mode("overwrite")
       .parquet("/Users/artur/Documents/GOLDEN_BOOK_Supply_Chain/RESULTS/inventory_periodic_by_script")
}
