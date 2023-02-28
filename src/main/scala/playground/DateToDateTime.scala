package playground

import org.apache.spark.sql.functions.{col, to_date, to_timestamp}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import playground.CreatingEmptyDFWithSchema.ddlSchemaStrSaCustTax

object DateToDateTime extends App {

  val spark = SparkSession.builder()
    .appName("DateToDateTime")
    .config("spark.master", "local")
    .config("spark.sql.codegen.wholeStage", "false")
    .getOrCreate()

  val schemaText = "`transaction_date` STRING"
  val schema = StructType.fromDDL(schemaText)

  val cars = Seq(
    Row("2023-01-10"),
    Row("2023-01-11"),
    Row("2023-01-12"),
    Row("2023-01-13"))

  val rows = spark.sparkContext.parallelize(cars)
  val df: DataFrame = spark.createDataFrame(rows, schema)

  df.select(to_date(col("transaction_date"),"yyyy-MM-dd").as("transaction_date"))
    .filter(col("transaction_date") > null)
    .show()

  df.sort()
}
