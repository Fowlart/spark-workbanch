package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

import java.util.Random


object GenerateLatestAptosDailySalesFile extends App {

  val spark = SparkSession.builder()
    .appName("GenerateLatestAptosDailySalesFile")
    .config("spark.master", "local")
    .getOrCreate()

  val random = new Random(47)

  val generateMPF_STATUS = () => {
    val possible_MPF_STATUS = Seq("MPF Enabled", "MPF Exempt")
    possible_MPF_STATUS(random.nextInt(possible_MPF_STATUS.length))
  }

  val generateMPF_SUPPRESSION_TYPE = () => {
    val possible_MPF_SUPPRESSION_TYPE = Seq("MPF_PARTIAL", "MPF_ALL")
    possible_MPF_SUPPRESSION_TYPE(random.nextInt(possible_MPF_SUPPRESSION_TYPE.length))
  }

  val generateMPF_PILOT = () => {
    random.nextBoolean()
  }

  val generateId = () => {
    System.nanoTime()
  }

  spark.udf.register("generateMPF_STATUS", generateMPF_STATUS)
  spark.udf.register("generateMPF_SUPPRESSION_TYPE", generateMPF_SUPPRESSION_TYPE)
  spark.udf.register("generateMPF_PILOT", generateMPF_PILOT)
  spark.udf.register("generateId", generateId)

  val aptosDailySales = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("inferSchema", "true")
    .load("/Users/artur/Documents/table_samples/aptos_daily_sales_sftp_src/EDW_Daily_Sales_PRE_20230221.txt")
    .withColumn("MPF_STATUS", expr("generateMPF_STATUS()"))
    .withColumn("MPF_SUPPRESSION_TYPE", expr("generateMPF_SUPPRESSION_TYPE()"))
    .withColumn("MPF_PILOT", expr("generateMPF_PILOT()"))
    .withColumn("MARKETPLACE_ORDER_ID", expr("generateId()"))
    .withColumn("ORIGINAL_MARKETPLACE_ORDER_ID", expr("generateId()"))

  aptosDailySales
    .repartition(1)
    .write
    .format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .mode("overwrite")
    .save("src/main/resources/data/aptos_daily_sales")
}
