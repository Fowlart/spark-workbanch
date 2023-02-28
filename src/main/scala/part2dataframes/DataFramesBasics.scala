package part2dataframes

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types._

object DataFramesBasics extends App {

  // creating session
  val spark = SparkSession
    .builder()
    .appName("DataFramesBasics")
    .config("spark.master","local")
    .getOrCreate()

  // car schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val cars = spark.read
    .format("json")
    .option("inferSchema", "true")
    .schema(carsSchema)
    .load("/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/cars.json")

  cars.show()
  // cars.printSchema()
  // every row - it is array of things
  // cars.take(10).foreach(row => println(row))

  /** Spark DataTypes */
  val longType = LongType

  /** Create dataframe from Seq of tuples */
  // tuples
  val myRows = Seq(("Lada Kalina", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "2005-01-01", "RU"))
  val myDf = spark.createDataFrame(myRows)
  // myDf.show() // no column names
  import spark.implicits._
  myRows.toDF("Name", "MPG", "Cylinders", "Displacement",
    "HP", "Weight", "Acceleration", "Year", "CountryOrigin").show()

  /** 1 - create my DF */
  val familyIncome = List(
    ("Artur", "Developer",3200L),
    ("Olena", "House-keeper",200L),
    ("Zenoviy", "Investor",5000L))

  val familyIncomeDf = familyIncome.toDF("name","occupation","income")

  familyIncomeDf.show()

  /** 2 - write DF into the disk */
  val pathToReadAndWriteExp = "/Users/artur/IdeaProjects/spark-essentials/src/main/resources/data/family_income"

  familyIncomeDf
    .write
    .mode("overwrite")
    .json(pathToReadAndWriteExp)

  val inheritFamilyIncomeDfSchema = familyIncomeDf.schema

  // names of the column should match to the names
  // of fields in incoming data
  val ownSchema = StructType(Array(
    StructField("name", StringType),
    StructField("occupation", StringType),
    StructField("income", LongType)))

  familyIncomeDf.printSchema()

  val downloadedDataSet = spark
    .read
    .schema(inheritFamilyIncomeDfSchema)
    .schema(ownSchema) // overwrite
    .json(pathToReadAndWriteExp)

  downloadedDataSet.show()
}
