package playground


object Collections extends App {

  // ArrayList analog
  val result: Array[String] = new Array[String](5)
  result.update(0,"zero")
  result.update(1,"one")

  println(result.mkString("Array(", ", ", ")"))

}
