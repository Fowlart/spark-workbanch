package playground.dataTransformations

import org.apache.spark.sql.SparkSession

object CsvToJsonString extends App{

  val spark = SparkSession.builder()
    .appName("Csv To Json string")
    .config("spark.master", "local")
    .getOrCreate()

  val usrAsnAudit = spark.read
    .option("inferSchema", "true")
    .option("header","true")
    .csv("src/main/resources/data/usr_asn_audit.csv")
  
  usrAsnAudit.show()
}
