package com.github.fyodiya

import org.apache.spark.sql.SparkSession

object Day20StructuredAPIOverview extends App {

  println("CH4: Structured API Overview - look into DataFrames and DataSets!")

  println(s"Scala version: ${util.Properties.versionNumberString}")
  val spark = SparkSession.builder().appName("tourDeSpark").master("local").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5") //recommended for local, default is 200?
  println(s"Session started on Spark version: ${spark.version}")


  // in Scala
  val df = spark.range(500).toDF("number")
//  df.select(df.col("number") + 10)
  df.select(df.col("number") + 10) //here we see an actual Spark command
    .show(40) //selected 40 rows and showed them on screen

//  For the most part, you’re likely to work with DataFrames. To Spark (in Scala), DataFrames are
  //simply Datasets of Type Row. The “Row” type is Spark’s internal representation of its optimized
  //in-memory format for computation. This format makes for highly specialized and efficient
  //computation because rather than using JVM types, which can cause high garbage-collection and
  //object instantiation costs, Spark can operate on its own internal format without incurring any of
  //those costs

//  Columns represent a simple type like an integer or string, a complex type like an array or map, or
  //a null value. Spark tracks all of this type information for you and offers a variety of ways, with
  //which you can transform columns.

//  A row is nothing more than a record of data. Each record in a DataFrame must be of type Row, as
  //we can see when we collect the following DataFrames. We can create these rows manually from
  //SQL, from Resilient Distributed Datasets (RDDs), from data sources, or manually from scratch

  // in Scala
  //collects all rows in our dataset and moves it to our program's memory
  val tinyRange = spark.range(2).toDF().collect()

  val arrRow = spark.range(10).toDF(colNames = "myNumber").collect()
  //now we can use regular Scala expressions
  println("First 3 elements of our array:")
  arrRow.take(3).foreach(println)

  arrRow.slice(2, 7).foreach(println)
  println("Tail of our array:")
  println(arrRow.last)

}
