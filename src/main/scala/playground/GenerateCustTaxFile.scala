package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object GenerateCustTaxFile extends App{

  val spark = SparkSession.builder()
    .appName("GenerateCustTaxFile")
    .config("spark.master", "local")
    .getOrCreate()

  // 1. Read the latest cust_tax file
  val custTax = spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("inferSchema", "true")
    .load("/Users/artur/Documents/table_samples/EDW_Daily_Cust_Tax_PRE_20230222.txt")

  // 2. Take random row
  val cust_tax_rand_row = custTax.sample(0.1).limit(1)

 // 3. change column DEST_ADDR_LINE_1 to "1234"
  val cust_tax_rand_row_2 = cust_tax_rand_row
    .withColumn("DEST_ADDR_LINE_1", lit("TEST ADDRESS"))

  // generate current date string in format YYYYMMDD
  val current_date = java.time.LocalDate.now().toString.replace("-", "")

  // 4. Write to file with the same name pattern as the original file but with the current date
  cust_tax_rand_row_2
    .repartition(1)
    .write
    .format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .mode("overwrite")
    .save(s"/Users/artur/Documents/table_samples/EDW_Daily_Cust_Tax_PRE_$current_date.txt")

}
