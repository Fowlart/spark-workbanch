package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{abs, approx_count_distinct, col, current_timestamp, datediff, round, to_date}
import org.apache.spark.sql.types.LongType

object ReservationDetailDataCheck extends App {

  val spark = SparkSession.builder()
    .appName("ReservationDetailDataCheck")
    .config("spark.master", "local")
    .config("spark.sql.codegen.wholeStage", "false")
    .getOrCreate()

  val reservationDetailJoined = spark
    .read
    .option("header","true")
    .option("inferSchema","true")
    .csv("/Users/artur/Documents/RESERVATION_DETAILS/JOINED_DATA")

  reservationDetailJoined

    .withColumn("DiffInHrs_1",
      (col("updateTimestamp").cast(LongType) -
        col("current_statusupdated_timestamp").cast(LongType)) / 3600)

    // may be different
    .withColumn("DiffInHrs_2",
     (col("createdTimeStamp").cast(LongType) -
        col("created_timestamp").cast(LongType)) / 3600)

    .withColumn("DiffInHrs_3",
     (col("u_no_show_timestamp").cast(LongType)
        - col("r_no_show_timestamp").cast(LongType)) / 3600)

    .withColumn("startDateTime",to_date(col("startDateTime")))

    //.select(col("DiffInHrs_2"))
    //.distinct()

    //.filter(datediff(col("reservation_date"),col("startDateTime"))>1)
    .filter(col("DiffInHrs_2").isNull)
    .show()

  reservationDetailJoined.select()
}
