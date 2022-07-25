package com.github.fyodiya

import org.apache.spark.sql.SparkSession

object Day20Exercise extends App {

  println(s"Scala version: ${util.Properties.versionNumberString}")
  val spark = SparkSession.builder().appName("tourDeSpark").master("local").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5") //recommended for local, default is 200?
  println(s"Session started on Spark version: ${spark.version}")

  //create a dataframe with a single column "JulyNumbers" from 1 to 31
  val range1To31 = spark.range(1, 32).toDF("JulyNumbers")

  //show all 31 numbers on a screen
  range1To31.show(31)

  //save it as a numbers.csv
  val ourNumbers = spark.range(31 +1).toDF().collect()
  val arrRow = spark.range(31).toDF(colNames = "JulyNumbers").collect()

  //create a new dataframe with numbers from 100 to 1300
  val dataFrame = spark.range(100, 1300 +1).toDF("Numbers")
  dataFrame.show(31)
  val DF100To3100 = dataFrame.select(dataFrame.col("Numbers"))

  //show last 5 numbers
 DF100To3100.tail(5).foreach(println)
  //range100To1300.reverse.take(5).foreach(println) //another way




}
