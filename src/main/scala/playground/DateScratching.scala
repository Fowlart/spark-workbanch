package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import java.sql.Date
import java.time.{Duration, LocalDate}

object DateScratching extends App {

  /**
    * Create a sequence containing all dates between from and to
    *
    * @param dateFrom The date from
    * @param dateTo   The date to
    * @return A Seq containing all dates in the given range
    */
  def getDateRange(
                    dateFrom: Date,
                    dateTo: Date
                  ): Seq[Date] = {
    val daysBetween = Duration
      .between(
        dateFrom.toLocalDate.atStartOfDay(),
        dateTo.toLocalDate.atStartOfDay()
      )
      .toDays

    val newRows = Seq.newBuilder[Date]
    // get all intermediate dates
    for (day <- 0L to daysBetween) {
      val date = Date.valueOf(dateFrom.toLocalDate.plusDays(day))
      newRows += date
    }
    newRows.result()
  }

  val spark = SparkSession.builder()
    .appName("DateScratching")
    .config("spark.master", "local")
    .config("spark.sql.codegen.wholeStage", "false")
    .getOrCreate()

  import spark.implicits._

  val sparseData = spark.sparkContext.parallelize(Seq(
    ("2019-01-01", 1),
    ("2019-01-12", 10),
    ("2019-01-14", 9),
    ("2019-01-15", 8),
    ("2019-01-20", 10),
    ("2019-01-25", 7),
    ("2019-01-31", 5)
  )).toDF("date", "stock")
    .withColumn("date", col("date").cast(DateType))

  val minMaxRow = sparseData.select("date")
    .agg(min("date").as("min"), max("date").as("max"))
    .distinct()
    .first()

  val allDates = spark.sparkContext.parallelize(getDateRange(minMaxRow.getDate(0),minMaxRow.getDate(1))).toDF("date")

  val joined = sparseData.join(allDates, Seq("date"), "outer")
    .sort("date")

  import org.apache.spark.sql.expressions.Window

  val window = Window
    // Note this windows over all the data as a single partition
    .orderBy("date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val filled = joined.withColumn(
    "stock",
    when(col("stock").isNull, last("stock", ignoreNulls = true).over(window)).otherwise(col("stock"))
  )

  filled.show()



}
