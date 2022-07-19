package com.github.fyodiya

import org.apache.spark.sql.SparkSession

import scala.io.StdIn.readLine

object Day17HelloSpark extends App {
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
  val divisibleBy10 = alsoRange.where("number % 10 = 0")
  //show the results
  divisibleBy10.show()

  //DataFrame = DataBase
  //just like a table, except some rows might be stored on different computers
  //of course, with 1 computer there is nothing where to distribute

  println(s"We have ${divisibleBy10.count()} numbers divisible by 10!")

  val flightData2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/resources/flight-data/csv/2015-summary.csv")

//  println(flightData2015.take(5).mkString(",")
  println(flightData2015.sort("count").take(10).mkString(","))

  flightData2015.sort("count").explain()


  readLine("Enter anything to stop Spark!")

  spark.stop() //or .close() if you want to stop the Spark engine before the program stops running
}
