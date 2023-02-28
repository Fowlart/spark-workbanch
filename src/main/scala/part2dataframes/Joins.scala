package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, max, col}

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // all values from guitaristsDF and only corresponding values from bandsDF
  guitaristsDF.as("guitarists")
    .join(bandsDF.as("bands"),col("bands.id")===col("guitarists.id"),"left_outer")
    .show()

  // all values from bandsDF and only corresponding from guitaristsDF
  guitaristsDF.as("guitarists")
    .join(bandsDF.as("bands"),col("bands.id")===col("guitarists.id"),"right_outer")
    .show()
}

