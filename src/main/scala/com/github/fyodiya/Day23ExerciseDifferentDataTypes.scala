package com.github.fyodiya

import org.apache.spark.sql.functions.col
import shapeless.syntax.std.tuple.unitTupleOps

object Day23ExerciseDifferentDataTypes extends App {

  println("Exercise with different data types.")
  val spark = SparkUtilities.getOrCreateSpark("Spark Sandbox")
  //  spark.conf.set("spark.sql.caseSensitive", true)

  //load 1st of March of 2011 into dataFrame
  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)

  df.printSchema()
  df.createOrReplaceTempView("dfTable")

  //get all purchases that have been made from Finland
  println("Purchases that have been made from Finland:")
  df.where(col("Country") === "Finland").show()

  //sort by Unit Price and LIMIT 20
  println("20 purchases, sorted by price of the unit:")
  df.sort("UnitPrice").show(20)

  //collect results into an Array of Rows
  //print these 20 Rows
  val arrayOfRows = df.sort("UnitPrice").limit(20).collect()

  println(arrayOfRows)

  //You can use either SQL or Spark or mis of syntax

}
