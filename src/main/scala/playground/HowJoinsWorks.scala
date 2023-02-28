package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HowJoinsWorks extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val familyIncome = List(
    (null: String, "Artur", "Developer",3200L),
    ("null", "Olena", "House-keeper",200L),
    ("3", "Zenoviy", "Investor",5000L),
    ("5", "Melanya", "Student",10L),
    ("5", "Milan", "Student",10L))

  val familyMembersDf = familyIncome.toDF("id", "name", "occupation", "income")

  val car = List((null: String, "Renault"), ("null", "BMW"), ("3", "Mazda"), ("4", "Volkswagen"))

  val carsDF = car.toDF("id", "car_name")

  val joinCondition = col("car.id") === col("family.id")

  // inner join
  familyMembersDf.as("family").join(carsDF.as("car"), joinCondition)

  //left join - will be not different from inner join here
  familyMembersDf.as("family")
    .join(carsDF.as("car"), joinCondition, "left")

  // left join - will be different because carDF more wider then familyIncomeDf
  // will produce results with nulls

  // with Volkswagen
  carsDF.as("car").join(familyMembersDf.as("family"),joinCondition,"left_outer")

  // with Melanya
  carsDF.as("car")
    .join(familyMembersDf.as("family"),joinCondition,"right_outer")

  // will get results as left_outer + right_outer(with Volkswagen and with Melanya)
  carsDF.as("car")
    .join(familyMembersDf.as("family"),joinCondition,"outer")

  // will see ONLY cars with same rows as in 'inner' case
  familyMembersDf.as("car")
    .join(carsDF.as("family"),joinCondition,"semi")

  // drop duplicates
  //assert(familyMembersDf.dropDuplicates(List("id", "name", "occupation", "income")).count()==5)
  //assert(familyMembersDf.dropDuplicates(List("id", "occupation", "income")).count()==4)

  // The question: is Olena stays without a car? Definitely, Artur will not have a car.
  // The answer: Olenka still has BMW => string "null" equal to string "null"
  familyMembersDf.as("family").join(carsDF.as("car"), joinCondition)

  // here come Artur with Renault => nulls where replaced by a value
  familyMembersDf.na.fill("null").as("family")
    .join(carsDF.na.fill("null").as("car"), joinCondition).show()

}
