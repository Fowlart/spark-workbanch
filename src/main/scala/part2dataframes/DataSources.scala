package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  val cars = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode","failFast") // (dropMalformed/permissive[default])
    .load("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/cars.json")
  // Writing DFs

  // JSON FLAGS
  val carsAlt = spark.read
    .schema(carsSchema)
    .option("mode","failFast")
    .option("dateFormat", "YYYY-MM-dd") // works ONLY with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // compression 8kb vs 73kb
  carsAlt.write.format("json")
    .mode(SaveMode.Overwrite)
    .option("compression", "gzip")
    .save("src/main/resources/data/cars_dupe_compressed")

  carsAlt.write.format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe")

  // uncompression
  val uncompressedCars = spark.read
    .schema(carsSchema)
    .option("mode","failFast")
    .option("dateFormat", "YYYY-MM-dd") // works ONLY with schema; if Spark fails parsing, it will put null
    .option("compression", "gzip")
    .json("src/main/resources/data/cars_dupe_compressed")

  println(uncompressedCars.schema.json)


}
