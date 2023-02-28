package playground

import org.apache.spark.sql.SparkSession

object SmartSheet extends App {

  val spark = SparkSession.builder()
    .appName("SmartSheet")
    .config("spark.master", "local")
    .config("spark.sql.codegen.wholeStage", "false")
    .getOrCreate()

  val smartSheet = spark
    .read
    .format("delta")
    .load("/Users/artur/Documents/SMART_SHEETS")

  smartSheet.show()

  println(smartSheet.count())

}
