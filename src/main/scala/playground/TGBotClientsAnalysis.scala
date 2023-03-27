package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

object TGBotClientsAnalysis extends App {

  val spark = SparkSession.builder()
    .appName("TGBotClientsAnalysis")
    .config("spark.master", "local")
    .getOrCreate()


  val clients = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/clients.json")
    .dropDuplicates("telegramFirstName", "telegramLastName", "bucket")

  /***
    * clients
    * .na
    * .fill("not_specified")
    * .withColumn("bucket_size", size(col("bucket")))
    * .write
    * .mode(SaveMode.Overwrite)
    * .json("src/main/resources/data/clients_enhanced") ***/

  clients
    .withColumn("row_number", row_number.over(Window.orderBy("firstName")))
    .show()

}
