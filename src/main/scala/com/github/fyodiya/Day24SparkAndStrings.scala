package com.github.fyodiya

object Day24SparkAndStrings extends App {

  println("Chapter 6: Working with strings!")
  val spark = SparkUtilities.getOrCreateSpark("Spark Sandbox")
  //  spark.conf.set("spark.sql.caseSensitive", true)

  val filePath = "src/resources/retail-data/by-day/2011-12-01.csv"
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)
  df.printSchema()

  df.createOrReplaceTempView("dfTable")


  //String manipulation shows up in nearly every data flow, and it’s worth explaining what you can
  //do with strings. You might be manipulating log files performing regular expression extraction or
  //substitution, or checking for simple string existence, or making all strings uppercase or
  //lowercase.
  //Let’s begin with the last task because it’s the most straightforward. The initcap function will
  //capitalize every word in a given string when that word is separated from another by a space.

}
