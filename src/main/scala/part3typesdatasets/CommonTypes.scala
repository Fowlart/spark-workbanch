package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  import spark.implicits._

  //1-adding a value to the DF as new column
  moviesDF.withColumn("new column",lit("new value"))

  //2-adding a value to existed column
  val row = List((null,
    "Artur",
    "ArtInc",
    10D,
    1000,
    "R",
    "Comedy",
    1000000L,
    "03-JUL-22",
    null,
    120,
    null,
    "Art Life",
    1000000L,
    1000000L,
    1000000L))

  val row_0 = List((null,
    "null",
    "ArtInc",
    10D,
    1000,
    "R",
    "Comedy",
    1000000L,
    "03-JUL-22",
    null,
    120,
    null,
    "Art Life",
    1000000L,
    1000000L,
    1000000L))

  // searching 'null' string
  row
    .toDF(moviesDF.columns: _*)
    .union(row_0.toDF())
    .filter(col("Title").isNull)
    .show()

  // correlation
  // println(s"Production_Budget and IMDB_Rating correlation: ${moviesDF.stat.corr("Production_Budget","IMDB_Rating")}")

  // Strings DataType
  moviesDF.filter(col("Director").isNotNull).select(upper(col("Director")))
  moviesDF.filter(col("Director").contains("Art")).select(upper(col("Director")))

  // regex
  val regEx = "art|ol"
  moviesDF.select(col("Director"),regexp_extract(col("Director"),regEx,0).as("extracted regex"))
    .filter(col("extracted regex").isNotNull and col("extracted regex")=!="")
    .show(10)


}
