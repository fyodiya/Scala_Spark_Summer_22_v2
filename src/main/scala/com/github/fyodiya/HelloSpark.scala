package com.github.fyodiya

import org.apache.spark.sql.SparkSession

object HelloSpark extends App {
  println(s"Testing Scala version: ${util.Properties.versionString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  //session is a common name for the above spark object
  println(s"Session started on Spark version ${spark.version}")

  val myRange = spark.range(1000).toDF("number") //creates a single column dataframe (table)
  val divisibleBy5 = myRange.where("number % 5 = 0") //similarities with SQL and regular Scala
  divisibleBy5.show(10) //shows first 10 rows

  //create range of numbers 0 to 100
  val alsoRange = spark.range(100).toDF("number")
  //filter into numbers divisible by 10
  val filteredNumbers = alsoRange.where("number % 10 = 0")
  //show the results
  filteredNumbers.show()

  spark.stop() //or .close() if you want to stop the Spark engine before the program stops running
}
