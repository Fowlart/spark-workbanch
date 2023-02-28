package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object SparkWhenClause extends App {

  val spark = SparkSession.builder()
    .appName("ReservationDetail")
    .config("spark.master", "local")
    .config("spark.sql.codegen.wholeStage", "false")
    .getOrCreate()

  val sc = spark.sparkContext

  val rdd = sc.parallelize((1 to 10))

  import spark.implicits._

  val numDf = rdd.toDF("number")

  numDf.withColumn("computed",
    when(col("number")===5, "5")
      .when(col("number").between(1, 4), "[1-4]")
        .when(col("number").between(6, 10), "[6-10]"))
    .show()

}
