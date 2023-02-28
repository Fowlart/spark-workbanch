package playground

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, current_date, date_add, date_sub, expr, lit, lower, max, to_timestamp}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, TimestampType}

import scala.util.{Failure, Success, Try}

object AggregationSimulator extends App {

  // helpers
  case class Metric(table: String, name: String, value: String = null)

  object MetricNames {
    val LAST_PROCESSING_DATE = "last_processing_date"
  }

  def applyDurationFilter(duration: Int, column: Either[String, Column]): Column = {
    if (duration > 0) {
      val v = column match {
        case Left(value) => col(value)
        case Right(value) => value
      }
      v.cast(DateType) >= date_sub(current_date(), duration + 1)
    } else {
      lit(true)
    }
  }

  def getLastMetricValue(metric: Metric): Option[String] = {
    // todo
    Option.empty
  }

  def getMaxColumnValue(df: DataFrame, column: String, spark: SparkSession): Option[String] = {
   import spark.implicits._
    Try(df(column)).toOption match {
      case Some(c) => df.agg(max(c)).as[String].take(1).headOption.flatMap(Option(_))
      case _ => None
    }
  }

  def updateLastProcessingDate(input: DataFrame, metricTableKey: String, incrementalColumn: String, sparkSession: SparkSession): Unit = {
    if (StringUtils.isNoneBlank(metricTableKey, incrementalColumn)) {
      val newLastProcessingDate = getMaxColumnValue(input, incrementalColumn, sparkSession)
      newLastProcessingDate match {
        case Some(v) =>
          val metric = Metric(metricTableKey, MetricNames.LAST_PROCESSING_DATE, v)
          println(s">>> Save metric[$metric] for $metricTableKey aggregation")
         // saveMetric(metric)
        case _ =>
          println(s">>> Failed to define max($incrementalColumn) for $metricTableKey")
      }
    } else {
      println(s">>> Failed to updateLastProcessingDate by incrementalColumn[$incrementalColumn] & metricTableKey[$metricTableKey]")
    }
  }
  // helpers: end

  def getMergeCondition(matchColumns: Seq[String], targetAlias: String, inputAlias: String): String = {
    val condition1 = matchColumns.take(2).map(col => s"$targetAlias.$col = $inputAlias.$col").mkString(" and ")
    val condition2 = matchColumns.map(col => s"$targetAlias.$col = $inputAlias.$col").mkString(" and ")
    val condition6 = matchColumns.takeRight(2).map(col => s"$inputAlias.$col is null").mkString(" and ")
    val condition7 = matchColumns.takeRight(3).map(col => s"$targetAlias.$col = $inputAlias.$col").mkString(" and ")
    val condition8 = matchColumns.take(1).map(col => s"$inputAlias.$col is null").mkString(" ")
    val matchCondition = s"""($condition2 ) or ($condition1 and $condition6) or ($condition7 and $condition8) """
    matchCondition
  }

  def unionDataFrames(df1: DataFrame, df2: DataFrame): Dataset[Row] = {
    def expression(myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map {
        case x if myCols.contains(x) => col(x)
        case x => lit(null).as(x)
      }
    }

    val colsDF1 = df1.columns.toSet
    val colsDF2 = df2.columns.toSet
    val allCols = colsDF1 ++ colsDF2 // union
    df1.select(expression(colsDF1, allCols): _*).unionByName(df2.select(expression(colsDF2, allCols): _*))
  }

  // creating session
  val spark = SparkSession
    .builder()
    .appName("Aggregation simulation")
    .config("spark.master","local")
    .getOrCreate()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold","-1")
  val incrementalColumnName = "dp_create_datetime"
  val shiptIncrColumnName= "dt"
  val instacartmetric = Metric("market_seller_mlog", MetricNames.LAST_PROCESSING_DATE)
  val shiptMetric = Metric("shipt_market_seller_tlog", MetricNames.LAST_PROCESSING_DATE)
  val withFilter = applyDurationFilter(-1, Left("dp_create_timestamp"))

  // Read source data for mlog, tlog, orderheader, orderline, ordertender, smtproduct
  val instacartMLog = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/instacart_marqueta.csv")
    .select("order_id", "approval_code", "dp_create_datetime", "transaction_date_pt","transaction_datetime_pt","store_location_code")
    .distinct()

  val instacartTLog = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/instacart_tlog.csv")
    .select("order_id", "user_id").distinct()

  val orderTenderDetails = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/order_tender_details_events.csv")
    .filter(withFilter)
    .select("order_number", "authorization_code")
    .where(col("authorization_code").isNotNull).distinct()

  val orderHeaderConsolidated = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/order_header_consolidated.csv")
    .filter(withFilter)
    .select("order_number", "usa_id", "sephora_id",
      "transaction_datetime", "sub_transaction_type", "transaction_number",
      "terminal_id", "dp_create_timestamp", "dp_update_timestamp", "store_number","order_submit_date").distinct()

  val orderLineConsolidated = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/order_line_consolidated.csv")
    .filter(withFilter)
    .select("order_number", "sku_number", "units", "tax_amount",
      "discount_local_amount", "net_sales_local_amount", "transaction_line_number", "original_order_number", "transaction_type")
    .distinct()

  val smtProduct = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/smtProduct.csv")
    .select("sku_number", "upc").distinct()

  // Get Instacart Historical Orders Group by market_seller_order_number,market_seller_client_id,market_seller_approval_code,
  // Order by Reporting Date desc where reconcile_flag = N
  val marketSellerHistorical =

  Try(spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/market_seller_order_details.csv"))

  match {
    case Success(hist) =>
      val allOrderNumbers = hist.select("sephora_order_number").distinct()
      val marketSellerOrderNumbers = hist.select(col("market_seller_order_number")).withColumnRenamed("market_seller_order_number","order_id")
        .where(col("sephora_order_number").isNull && col("reconcile_flag") === "N")
      (allOrderNumbers,marketSellerOrderNumbers)
    case Failure(exception) =>
      println(">>> Aggregation: Historical Not found" + ExceptionUtils.getStackTrace(exception))
      println(s">>> Historical DB has no data for ${"market_seller_order_details"}")
      (spark.emptyDataFrame, spark.emptyDataFrame)
  }

  val instacartMaxSourceLoadDate = getLastMetricValue(instacartmetric)

  // Get mlog Incremental based on dp_create_datetime
  val instacartMLogIncremental = instacartMaxSourceLoadDate match {
    case Some(v) if v != null =>
      println(s">>> ----The max_date for instacart is ----- $v")
      if (!marketSellerHistorical._2.isEmpty) {
        val mlogHistorical = instacartMLog.join(marketSellerHistorical._2, "order_id")
        unionDataFrames(instacartMLog.filter(
          col(incrementalColumnName).cast(TimestampType) > to_timestamp(lit(v))),
          mlogHistorical).distinct()
      } else {
        instacartMLog.filter(
          col(incrementalColumnName).cast(TimestampType) > to_timestamp(lit(v))).distinct()
      }
    case _ => instacartMLog
  }

  // Left Join mlog Incremental and tlog
  val instaCartOrderLog = instacartMLogIncremental.as("mlog")
    .join(instacartTLog.as("tlog"),
      col("tlog.order_id") === col("mlog.order_id"), "left")
    .select(col("mlog.order_id"), col("tlog.user_id"), col("mlog.approval_code"), col("mlog.transaction_datetime_pt"),
      col("mlog.transaction_date_pt"),col("mlog.store_location_code")).distinct()
  println(s">>> Instacart Order Log Count: ${instaCartOrderLog.count()}")
  // Get Instacart Orders from SDS having sub_transaction_type having Instacart
  val instacartDPOrders = orderHeaderConsolidated
    .filter(lower(col("sub_transaction_type")).contains("instacart")).as("header")
    .join(orderLineConsolidated.as("line"),
      col("line.order_number") === col("header.order_number"))
    .join(orderTenderDetails.as("tender"),
      col("tender.order_number") === col("header.order_number"),"left")
    .join(smtProduct.as("smtPrd"),
      col("line.sku_number") === col("smtPrd.sku_number"), "left")
    .select("tender.authorization_code","header.order_number", "original_order_number", "usa_id", "sephora_id",
      "transaction_datetime", "transaction_type", "transaction_number",
      "terminal_id", "dp_create_timestamp", "dp_update_timestamp", "store_number", "line.sku_number", "units", "tax_amount",
      "discount_local_amount", "net_sales_local_amount", "transaction_line_number", "smtPrd.upc")
    .withColumnRenamed("authorization_code", "market_seller_approval_code")
    .withColumnRenamed("order_number", "sephora_order_number")
    .withColumnRenamed("original_order_number", "sephora_original_order_number")
    .withColumnRenamed("usa_id", "bi_usa_id")
    .withColumnRenamed("transaction_datetime", "transaction_timestamp")
    .withColumnRenamed("net_sales_local_amount", "line_total")
    .withColumnRenamed("transaction_number", "sephora_transaction_number")
    .withColumnRenamed("transaction_line_number", "sephora_transaction_line_number")
    .withColumnRenamed("terminal_id", "sephora_terminal_id")
    .withColumnRenamed("tax_amount", "tax")
    .withColumnRenamed("discount_local_amount", "discount")
    .withColumnRenamed("dp_create_timestamp", "dp_create_date")
    .withColumnRenamed("dp_update_timestamp", "dp_update_date")
    .withColumn("reconcile_flag", lit("N"))

  println(s">>> instacartDPOrders completed" )

  val sdsReturnOrders = orderLineConsolidated.as("line")
    .filter(col("line.transaction_type") === "R")
    .join(orderHeaderConsolidated.as("header"),
      col("line.order_number") === col("header.order_number"))
    .join(orderHeaderConsolidated.as("headerOriginal"),
      col("headerOriginal.order_number") === col("line.original_order_number"),
      "left")
    .join(orderTenderDetails.as("tender"),
      col("tender.order_number") === col("header.order_number"),"left")
    .join(smtProduct.as("smtPrd"),
      col("line.sku_number") === col("smtPrd.sku_number"), "left")
    .filter(lower(col("headerOriginal.sub_transaction_type")).contains("instacart")
      && col("headerOriginal.order_number").isNotNull)
    .select("tender.authorization_code","header.order_number", "header.usa_id", "header.sephora_id",
      "header.transaction_datetime", "line.transaction_type", "header.transaction_number",
      "header.terminal_id", "header.dp_create_timestamp", "line.original_order_number",
      "header.dp_update_timestamp", "header.store_number", "line.sku_number", "line.units", "line.tax_amount",
      "line.discount_local_amount", "line.net_sales_local_amount", "line.transaction_line_number", "smtPrd.upc")
    .withColumnRenamed("authorization_code", "market_seller_approval_code")
    .withColumnRenamed("order_number", "sephora_order_number")
    .withColumnRenamed("usa_id", "bi_usa_id")
    .withColumnRenamed("transaction_datetime", "transaction_timestamp")
    .withColumnRenamed("net_sales_local_amount", "line_total")
    .withColumnRenamed("transaction_number", "sephora_transaction_number")
    .withColumnRenamed("transaction_line_number", "sephora_transaction_line_number")
    .withColumnRenamed("terminal_id", "sephora_terminal_id")
    .withColumnRenamed("tax_amount", "tax")
    .withColumnRenamed("discount_local_amount", "discount")
    .withColumnRenamed("dp_create_timestamp", "dp_create_date")
    .withColumnRenamed("dp_update_timestamp", "dp_update_date")
    .withColumnRenamed("original_order_number", "sephora_original_order_number")
    .withColumn("reconcile_flag", lit("N"))

  println(s">>> sdsReturnOrders Completed")
  // Join orderlogcombined(Historical Non reconciled + Incremental tlog and mlog orders) with
  // approval_code = tender.authorization_code
  // Join orderheader.order_number, orderline.order_number
  val sdsOrderTablesJoined = orderTenderDetails.as("tender")
    .join(orderHeaderConsolidated.as("header"),
      col("header.order_number") === col("tender.order_number")
        && lower(col("header.sub_transaction_type")).contains("instacart"))
    .join(orderLineConsolidated.as("line"),
      col("line.order_number") === col("header.order_number"))
    .join(smtProduct.as("smtPrd"),
      col("line.sku_number") === col("smtPrd.sku_number"), "left")
    .select("header.order_number", "header.usa_id", "header.sephora_id",
      "header.transaction_datetime", "line.transaction_type", "header.transaction_number",
      "header.terminal_id", "header.dp_create_timestamp", "line.original_order_number",
      "header.dp_update_timestamp", "header.store_number", "line.sku_number", "line.units", "line.tax_amount",
      "line.discount_local_amount", "line.net_sales_local_amount", "line.transaction_line_number",
      "smtPrd.upc","tender.authorization_code","header.order_submit_date")

  println(s">>> sdsOrderTablesJoined Completed")

  val instacartOrderDetails = instaCartOrderLog.as("instaOrder")
    .join(sdsOrderTablesJoined.as("sdsOrderTables"),
      col("sdsOrderTables.authorization_code") === col("instaOrder.approval_code")
        && (col("sdsOrderTables.order_submit_date")
        .between(date_sub(col("instaOrder.transaction_date_pt"),1),date_add(col("instaOrder.transaction_date_pt"),1))
        || col("sdsOrderTables.order_submit_date").isNull)
        && col("sdsOrderTables.store_number") === col("instaOrder.store_location_code"), "left")
    .select(col("sdsOrderTables.order_number").as("sephora_order_number"),
      col("sdsOrderTables.original_order_number").as("sephora_original_order_number"),
      col("instaOrder.order_id").as("market_seller_order_number"),
      col("instaOrder.user_id").as("market_seller_client_id"),
      col("instaOrder.approval_code").as("market_seller_approval_code"),
      col("sdsOrderTables.usa_id").as("bi_usa_id"),
      col("sdsOrderTables.sephora_id").as("sephora_id"),
      coalesce(col("instaOrder.transaction_datetime_pt"),col("sdsOrderTables.transaction_datetime")).as("transaction_timestamp"),
      col("sdsOrderTables.store_number").as("store_number"),
      col("sdsOrderTables.transaction_type").as("transaction_type"),
      col("sdsOrderTables.sku_number").as("sku_number"),
      col("sdsOrderTables.upc").as("upc"),
      col("sdsOrderTables.units").as("units"),
      col("sdsOrderTables.tax_amount").as("tax"),
      col("sdsOrderTables.discount_local_amount").as("discount"),
      col("sdsOrderTables.net_sales_local_amount").as("line_total"),
      col("sdsOrderTables.transaction_number").as("sephora_transaction_number"),
      col("sdsOrderTables.terminal_id").as("sephora_terminal_id"),
      col("sdsOrderTables.transaction_line_number").as("sephora_transaction_line_number"),
      col("sdsOrderTables.dp_create_timestamp").as("dp_create_date"),
      col("sdsOrderTables.dp_update_timestamp").as("dp_update_date"),
      expr("case when sdsOrderTables.order_number is null then 'N' else 'Y' end as reconcile_flag"))

  println(s">>> instacartOrderDetails Completed ")
  // Get list of order_numbers to be excluded from SDS Only Orders
  val instacartDPOrderNumbers = unionDataFrames(instacartOrderDetails.select("sephora_order_number"), marketSellerHistorical._1).distinct()

  // Get final list of SDS Only Orders
  val finalInstacartDPOrders = unionDataFrames(instacartDPOrders, sdsReturnOrders).distinct()
    .as("left").join(instacartDPOrderNumbers.as("right"),
    col("left.sephora_order_number") === col("right.sephora_order_number"), "left_anti")

  println(s">>> finalInstacartDPOrders Completed ${finalInstacartDPOrders.count()}")
  val instaCartfinalDF = unionDataFrames(instacartOrderDetails, finalInstacartDPOrders).dropDuplicates("market_seller_order_number", "market_seller_approval_code", "sephora_transaction_line_number", "sephora_order_number").withColumn("market_seller",lit("instacart"))

  println(s">>> instacart aggregation completed  Completed row count is ${instaCartfinalDF.count()}")
  //Shipt related changes starts here
  val shiptTlogfile = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/shipt_tlog.csv")
    .select("order_id","sephora_order_number","dt")
    .withColumn("user_id",lit(null))
    .withColumn("approval_code",lit(null)).withColumn("transaction_datetime_pt",lit(null)).distinct()

    println(s">>> shipt file loaded")
  val marketSellerMaxSourceLoadDate = getLastMetricValue(shiptMetric)
  val shiptOrderLog = marketSellerMaxSourceLoadDate match {
    case Some(v) if v != null =>
      println(s">>> ----The max_date for shipt is ----- $v")
      if (!marketSellerHistorical._2.isEmpty) {
        val tlogHistorical = shiptTlogfile.join(marketSellerHistorical._2, "order_id")
        unionDataFrames(shiptTlogfile.filter(
          col(shiptIncrColumnName).cast(TimestampType) > to_timestamp(lit(v))),
          tlogHistorical).distinct()
      } else {
        shiptTlogfile.filter(
          col(shiptIncrColumnName).cast(TimestampType) > to_timestamp(lit(v))).distinct()
      }
    case _ => shiptTlogfile
  }
  println(s">>> shipt order log loaded ")
  val shiptDPOrders = orderHeaderConsolidated
    .filter(lower(col("sub_transaction_type")).contains("shipt")).as("header")
    .join(orderLineConsolidated.as("line"),
      col("line.order_number") === col("header.order_number"))
    .join(orderTenderDetails.as("tender"),
      col("tender.order_number") === col("header.order_number"),"left")
    .join(smtProduct.as("smtPrd"),
      col("line.sku_number") === col("smtPrd.sku_number"), "left")
    .select("tender.authorization_code","header.order_number", "original_order_number", "header.usa_id", "header.sephora_id",
      "header.transaction_datetime", "transaction_type", "header.transaction_number",
      "header.terminal_id", "header.dp_create_timestamp", "header.dp_update_timestamp", "header.store_number", "line.sku_number", "units", "tax_amount",
      "discount_local_amount", "net_sales_local_amount", "transaction_line_number", "smtPrd.upc")
    .withColumnRenamed("authorization_code", "market_seller_approval_code")
    .withColumnRenamed("order_number", "sephora_order_number")
    .withColumnRenamed("original_order_number", "sephora_original_order_number")
    .withColumnRenamed("usa_id", "bi_usa_id")
    .withColumnRenamed("transaction_datetime", "transaction_timestamp")
    .withColumnRenamed("net_sales_local_amount", "line_total")
    .withColumnRenamed("transaction_number", "sephora_transaction_number")
    .withColumnRenamed("transaction_line_number", "sephora_transaction_line_number")
    .withColumnRenamed("terminal_id", "sephora_terminal_id")
    .withColumnRenamed("tax_amount", "tax")
    .withColumnRenamed("discount_local_amount", "discount")
    .withColumnRenamed("dp_create_timestamp", "dp_create_date")
    .withColumnRenamed("dp_update_timestamp", "dp_update_date")
    .withColumn("reconcile_flag", lit("N"))

  val sdsReturnOrdersShipt = orderLineConsolidated.as("line")
    .filter(col("line.transaction_type") === "R")
    .join(orderHeaderConsolidated.as("header"),
      col("line.order_number") === col("header.order_number"))
    .join(orderHeaderConsolidated.as("headerOriginal"),
      col("headerOriginal.order_number") === col("line.original_order_number"),
      "left")
    .join(orderTenderDetails.as("tender"),
      col("tender.order_number") === col("header.order_number"),"left")
    .join(smtProduct.as("smtPrd"),
      col("line.sku_number") === col("smtPrd.sku_number"), "left")
    .filter(lower(col("headerOriginal.sub_transaction_type")).contains("shipt")
      && col("headerOriginal.order_number").isNotNull)
    .select("tender.authorization_code","header.order_number", "header.usa_id", "header.sephora_id",
      "header.transaction_datetime", "line.transaction_type", "header.transaction_number",
      "header.terminal_id", "header.dp_create_timestamp", "line.original_order_number",
      "header.dp_update_timestamp", "header.store_number", "line.sku_number", "line.units", "line.tax_amount",
      "line.discount_local_amount", "line.net_sales_local_amount", "line.transaction_line_number", "smtPrd.upc")
    .withColumnRenamed("authorization_code", "market_seller_approval_code")
    .withColumnRenamed("order_number", "sephora_order_number")
    .withColumnRenamed("usa_id", "bi_usa_id")
    .withColumnRenamed("transaction_datetime", "transaction_timestamp")
    .withColumnRenamed("net_sales_local_amount", "line_total")
    .withColumnRenamed("transaction_number", "sephora_transaction_number")
    .withColumnRenamed("transaction_line_number", "sephora_transaction_line_number")
    .withColumnRenamed("terminal_id", "sephora_terminal_id")
    .withColumnRenamed("tax_amount", "tax")
    .withColumnRenamed("discount_local_amount", "discount")
    .withColumnRenamed("dp_create_timestamp", "dp_create_date")
    .withColumnRenamed("dp_update_timestamp", "dp_update_date")
    .withColumnRenamed("original_order_number", "sephora_original_order_number")
    .withColumn("reconcile_flag", lit("N"))
  println(">>> sdk return order shipt")
  val sdsOrderTablesJoinedShipt = orderTenderDetails.as("tender")
    .join(orderHeaderConsolidated.as("header"),
      col("header.order_number") === col("tender.order_number")
        && lower(col("header.sub_transaction_type")).contains("shipt"))
    .join(orderLineConsolidated.as("line"),
      col("line.order_number") === col("header.order_number"))
    .join(smtProduct.as("smtPrd"),
      col("line.sku_number") === col("smtPrd.sku_number"), "left")
    .select("header.order_number", "header.usa_id", "header.sephora_id",
      "header.transaction_datetime", "line.transaction_type", "header.transaction_number",
      "header.terminal_id", "header.dp_create_timestamp", "line.original_order_number",
      "header.dp_update_timestamp", "header.store_number", "line.sku_number", "line.units", "line.tax_amount",
      "line.discount_local_amount", "line.net_sales_local_amount", "line.transaction_line_number",
      "smtPrd.upc","tender.authorization_code","header.order_submit_date")

  val shiptOrderDetails = shiptOrderLog.as("shipt")
    .join(sdsOrderTablesJoinedShipt.as("sdsOrderTables"),col("shipt.sephora_order_number") === col("sdsOrderTables.order_number"), "left")
    .select(
      coalesce(col("sdsOrderTables.order_number"),col("shipt.sephora_order_number")).as("sephora_order_number"),
      col("sdsOrderTables.original_order_number").as("sephora_original_order_number"),
      col("shipt.order_id").as("market_seller_order_number"),
      col("shipt.user_id").as("market_seller_client_id"),
      col("shipt.approval_code").as("market_seller_approval_code"),
      col("sdsOrderTables.usa_id").as("bi_usa_id"),
      col("sdsOrderTables.sephora_id").as("sephora_id"),
      coalesce(col("shipt.transaction_datetime_pt"),col("sdsOrderTables.transaction_datetime")).as("transaction_timestamp"),
      col("sdsOrderTables.store_number").as("store_number"),
      col("sdsOrderTables.transaction_type").as("transaction_type"),
      col("sdsOrderTables.sku_number").as("sku_number"),
      col("sdsOrderTables.upc").as("upc"),
      col("sdsOrderTables.units").as("units"),
      col("sdsOrderTables.tax_amount").as("tax"),
      col("sdsOrderTables.discount_local_amount").as("discount"),
      col("sdsOrderTables.net_sales_local_amount").as("line_total"),
      col("sdsOrderTables.transaction_number").as("sephora_transaction_number"),
      col("sdsOrderTables.terminal_id").as("sephora_terminal_id"),
      col("sdsOrderTables.transaction_line_number").as("sephora_transaction_line_number"),
      col("sdsOrderTables.dp_create_timestamp").as("dp_create_date"),
      col("sdsOrderTables.dp_update_timestamp").as("dp_update_date"),
      expr("case when sdsOrderTables.order_number is null then 'N' else 'Y' end as reconcile_flag"))

  val shiptDPOrderNumbers = unionDataFrames(shiptOrderDetails.select("sephora_order_number"), marketSellerHistorical._1).distinct()
  val finalShiptDPOrders = unionDataFrames(shiptDPOrders, sdsReturnOrdersShipt).distinct()
    .as("left").join(shiptDPOrderNumbers.as("right"),
    col("left.sephora_order_number") === col("right.sephora_order_number"), "left_anti")
  val shiptFinalDF = unionDataFrames(shiptOrderDetails, finalShiptDPOrders).dropDuplicates("market_seller_order_number", "market_seller_approval_code", "sephora_transaction_line_number", "sephora_order_number").withColumn("market_seller", lit("shipt"))
  spark.conf.set("overwriteSchema", "true")
  println(s">>> added mergeSchema true")

  println(s">>> shipt aggregation completed with row count ${shiptFinalDF}")
  val  finalDF = unionDataFrames(instaCartfinalDF,shiptFinalDF)
  println(s">>> instacart matric table ${instacartmetric.table}")
  updateLastProcessingDate(instacartMLogIncremental, instacartmetric.table, incrementalColumnName,spark)
  updateLastProcessingDate(shiptOrderLog, shiptMetric.table, shiptIncrColumnName,spark)

  println(s">>> final data frame count ${finalDF.count()}")
  println(">>> aggregation completed")

  val result = finalDF.select(
    col("line_total").cast(DoubleType),
    col("sephora_terminal_id").cast(StringType),
    col("tax").cast(DoubleType),
    col("sephora_original_order_number").cast(StringType),
    col("store_number").cast(StringType),
    col("upc").cast(LongType),
    col("dp_update_date").cast(StringType),
    col("sephora_id").cast(LongType),
    col("bi_usa_id").cast(LongType),
    col("sephora_order_number").cast(StringType),
    col("market_seller_client_id").cast(StringType),
    col("sephora_transaction_line_number").cast(StringType),
    col("units").cast(DoubleType),
    col("sku_number").cast(StringType),
    col("market_seller_order_number").cast(LongType),
    col("reconcile_flag").cast(StringType),
    col("transaction_type").cast(StringType),
    col("dp_create_date").cast(StringType),
    col("transaction_timestamp").cast(StringType),
    col("discount").cast(DoubleType),
    col("market_seller_approval_code").cast(StringType),
    col("sephora_transaction_number").cast(StringType),
    col("market_seller").cast(StringType)
  )
    println(s">>> final data frame schema ${result.schema.json}")

  result
    .write
    .mode("overwrite")
    .option("header","true")
    .parquet("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/aggregation_simulator/")
  }
