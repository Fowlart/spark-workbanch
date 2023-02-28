package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object StringFunctions extends App {

  val spark = SparkSession.builder()
    .appName("StringFunctions")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._


  val familyIncome = List(
    ("Artur", "Developer",3200L),
    ("Olena", "House-keeper",200L),
    ("Zenoviy", "Investor",5000L))

  val familyIncomeDf = familyIncome.toDF("name","occupation","income")

  familyIncomeDf
    .select(
      substring(col("name"),-2,4),
      expr("substr(CAST(name AS STRING), 2)").as("substr"),
      lpad(col("name"),8,"0"),
      date_format(lit("2022-11-28"), "yyyyMMdd")
    )
    //.show()

 familyIncomeDf
   .withColumn("wrong_casted",col("name").cast(IntegerType))
   .show()
}