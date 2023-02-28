package playground

import org.apache.spark.sql.SparkSession

object MutateEmployeeDF extends App {

  val spark = SparkSession.builder()
    .appName("MutateEmpoyeeDF")
    .config("spark.master", "local")
    .getOrCreate()

  val employeeDF = spark.read.option("inferSchema", "true").option("header", "true").csv("/Users/artur/IdeaProjects/dp-analytics-2.0/aggregation/src/test/resources/feeds/sa_sales_detail/employee")

  employeeDF.select("Employee_SK","Employee_ID").write.mode( "overwrite").option("header", "true").csv("/Users/artur/IdeaProjects/dp-analytics-2.0/aggregation/src/test/resources/feeds/sa_sales_detail/employee_trunc")


}
