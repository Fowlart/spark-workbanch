package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

 moviesDF
   .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy")
     .as("Actual_Release"))


  moviesDF
    .withColumn("current_date",current_date())
    .withColumn("date_of_start", to_date(lit("01-03-2019"),"dd-MM-yyyy"))
    .withColumn("date_diff_year",datediff(col("current_date"),col("date_of_start"))/365L)
    .select(
      "current_date",
      "date_of_start",
      "date_diff_year")
    .show(10)

  /**
    * Exercise
    * 1. How do we deal with multiple date formats?
    **/
  moviesDF
    .select(col("Title"),
      to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release_dd-MMM-yy"),
      to_date(col("Release_Date"), "dd-mm-yy").as("Actual_Release_dd-mm-yy"))

  // 2 -structs
  // will show data in json
  moviesDF.select(struct(
    to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release_dd-MMM-yy"),
    to_date(col("Release_Date"), "dd-mm-yy").as("Actual_Release_dd-mm-yy")
  ).as("date_struct"))

  // will show data in json
  val converted = moviesDF.select(struct(
    col("US_Gross"),
    col("Worldwide_Gross")).as("Gross_Struct"))

  // call separate field from the struct type
  converted.select(col("Gross_Struct").getField("Worldwide_Gross"))

  // select expr usage
  // creating struct
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit_Struct")
    // select field in struct
    .selectExpr("Profit_Struct.Worldwide_Gross")

  // arrays
  moviesDF.select(
    // forming an array
    array(col("US_Gross"), col("Worldwide_Gross")).as("Gross_Array"))
    // getting first element of array
    .selectExpr("Gross_Array", "Gross_Array[0]")
    .select(
      // functions with array
      size(col("Gross_Array")).as("array_size"),
      array_contains(col("Gross_Array"),1l).as("array_contains"))
    .show()

}
