package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting
  // filter Dramas
  println(s"Qty of dramas: ${moviesDF.filter(col("Major_Genre") === "Drama").count()}")

  // wrong Dramas filter
  // moviesDF.select(col("Major_Genre") === "Drama").show()

  val dataFrameWithOnlyOneValue = moviesDF.selectExpr("count(Major_Genre)") // will EXCLUDE nulls

  val countOfAllMoviesNullsIncluded = moviesDF.selectExpr("count(*)") // will INCLUDE nulls

  // separate Genres without nulls
  // moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // Approximate functions for really huge dataframes
  //moviesDF.select(count_distinct(col("Major_Genre")))// excludes nulls
  println(s"distinct.count: ${moviesDF.select(col("Major_Genre")).distinct().count()}") // includes nulls

  // moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()
  // moviesDF.select(min("Production_Budget").as("Minimum of production budget")).show()
  // moviesDF.select(max("Production_Budget").as("Maximum of production budget")).show()
  // moviesDF.select(sum("US_Gross").as("summary of us_gross"), avg("US_Gross").as("avg of us_gross")).show()

  // data science
  moviesDF.select(stddev(col("IMDB_Rating")).as("deviation from avg rating value"),
    mean(col("IMDB_Rating")),avg(col("IMDB_Rating")))

  /**
     =Grouping=
    */

  val countedByGenre = moviesDF
    .groupBy(col("Major_Genre")) // INCLUDES nulls
    .count()

  val avgByRating = moviesDF.groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  // many aggregations
  val aggregates = moviesDF.groupBy(col("Major_Genre")).agg(
    count("*").as("count of rows"),
    avg("IMDB_Rating").as("avg IMDB_Rating"))
    .orderBy(col("avg IMDB_Rating"))

  /** Compute the average IMDB rating and the average US gross revenue PER DIRECTOR */

 val bestDIRECTOR = moviesDF.groupBy(col("Director")).agg(
    avg("IMDB_Rating").as("avg IMDB_Rating"),
    avg("US_Gross").as("avg US_Gross")
  ).orderBy("avg US_Gross")

  bestDIRECTOR.show()
}
