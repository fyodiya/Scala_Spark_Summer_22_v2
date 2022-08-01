package com.github.fyodiya

import org.apache.spark.sql.functions.{col, desc}

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
  arrayOfRows.foreach(println)

  //You can use either SQL or Spark or mix of both


  val dff2011Finland=df.where(col("Country").equalTo("Finland"))
    .orderBy(desc("UnitPrice"))
    .limit(20).collect()
  dff2011Finland.foreach(println)


  df.createOrReplaceTempView("marchTable")
  val finlandRows20 =  spark.sql("SELECT * FROM marchTable WHERE Country = 'Finland' ORDER BY UnitPrice LIMIT 20").collect()
  for ((row, i) <- finlandRows20.zipWithIndex) {
    println(s"Row No. ${i+1} -> $row")
  }
  finlandRows20.take(5).foreach(println)
}
