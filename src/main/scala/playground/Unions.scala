package playground

import org.apache.spark.sql.SparkSession

object Unions extends App {

  val spark = SparkSession.builder()
    .appName("Unions")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val df1 = Seq((1, 2, 3)).toDF("col0", "col1", "col2")
  val df2 = Seq((4, 5, 6)).toDF("col0", "col1", "col3")

  df1.unionByName(df2).show

//  df1.union(df2).show()
}
