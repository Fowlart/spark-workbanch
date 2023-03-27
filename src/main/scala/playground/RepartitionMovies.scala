package playground

import org.apache.spark.sql.{SaveMode, SparkSession}

object RepartitionMovies extends App{

  val spark = SparkSession.builder()
    .appName("RepartitionMovies")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF
    .repartition(3)
    .write
    .mode(SaveMode.Overwrite)
    .json("src/main/resources/data/movies_repartitioned")



}
