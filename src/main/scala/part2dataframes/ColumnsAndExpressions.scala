package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")

  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // chain filters
  val americanPowerfulCarsDF2 = carsDF.filter(
    col("Origin") === "USA"
      and
      col("Horsepower") > 150)

  val americanPowerfulCarsDF3 = carsDF
    .filter("Origin = 'USA' and Horsepower > 150")

  // union = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  /**
    * Exercises
    *
    * 1. Read the movies DF and select 2 columns of your choice
    * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
    * 3. Select all COMEDY movies with IMDB rating above 6
    *
    * Use as many versions as possible
    */

  val moviesDF = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.show()

  // 2 -> Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
  val moviesDfWithTotalProfit = moviesDF.filter(
    col("US_Gross").isNotNull and
      col("Worldwide_Gross").isNotNull and
      col("US_DVD_Sales").isNotNull
  )
    .withColumn("Total_Profit",
      col("US_Gross")
        + col("Worldwide_Gross")
        + col("US_DVD_Sales"))

  moviesDfWithTotalProfit.show(10)

  val columnsAll: Array[String] = moviesDF.columns.map(m=>col(m).toString())

  val moviesDfWithTotalProfitNulls = moviesDF
    .na
    .fill(0,columnsAll)
    .withColumn("Total_Profit",

    col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales"))

  moviesDfWithTotalProfitNulls.show()

  println(s"moviesDfWithTotalProfit.count: ${moviesDfWithTotalProfit.count} " +
    s"| moviesDfWithTotalProfitNulls.count: ${moviesDfWithTotalProfitNulls.count}")

  // 3 -> Select all COMEDY movies with IMDB rating above 6
  val comedyWithHighRating = moviesDfWithTotalProfitNulls
    .filter( (col("Major_Genre")==="Comedy").and( col("IMDB_Rating")>6) )

  println(s"Good comedies count: ${comedyWithHighRating.count}")

  /** Selecting column technics*/

  val columnSelecting = comedyWithHighRating
    .select(col("Major_Genre"),
      column("Major_Genre"),
      'Major_Genre, // scala symbol
      $"Major_Genre", // funny
      expr("Major_Genre"))

  columnSelecting.show()
}
