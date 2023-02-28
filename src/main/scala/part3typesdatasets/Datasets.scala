package part3typesdatasets

import org.apache.spark.api.java.function.ReduceFunction
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

import java.sql.Date

object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  implicit val intEncoder = Encoders.scalaInt

  // this is distributed collection
  val intDataset: Dataset[Int] = numbersDF.as[Int]

  // work as with regular scala collection
  intDataset.map(it => it * 2).foreach(it => println(s"number:=>[$it]"))

  // working with a complex type
  // all case classes extends product
  case class Car(Name: String,
                 Miles_per_Gallon: Option[Double],
                 Cylinders: Long,
                 Displacement: Double,
                 Horsepower: Option[Long],
                 Weight_in_lbs: Long,
                 Acceleration: Double,
                 Year: String,
                 Origin: String)

  implicit val carEncoder: Encoder[Car] = Encoders.product[Car]

  val carsDataset = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/cars.json").as[Car]

  println("Cars avg hrs_power: ")
  //carsDataset.reduce(ReduceFunction.)

}
