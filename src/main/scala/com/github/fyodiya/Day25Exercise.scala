package com.github.fyodiya

import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, datediff, lit, months_between, to_date}

object Day25Exercise extends App {

  println("Day 24: Exercise with strings!")
  val spark = SparkUtilities.getOrCreateSpark("String Sandbox")

  //open up March 1st, of 2011 CSV
  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"
  val df = SparkUtilities.readDataWithView(spark, filePath)

  //Add new column with current date
  val dateDF = spark.range(10)
    .withColumn("today", current_date())
  dateDF.createOrReplaceTempView("dateTable")
  dateDF.printSchema()

  //Add new column with current timestamp
  val timeStampDF = spark.range(10)
    .withColumn("now", current_timestamp())
  timeStampDF.printSchema()

  //add new column which contains days passed since InvoiceDate (here it is March 1, 2011 but it could vary)
  dateDF.select(
    to_date(lit("2011-03-01")).alias("start"),
    to_date(lit("2022-08-02")).alias("end"))
    .withColumn("dayDifference", datediff(col("end"), col("start")))
  .show(10, truncate = false)

  //add new column with months passed since InvoiceDate
  dateDF.select(
    to_date(lit("2011-03-01")).alias("start"),
    to_date(lit("2022-08-02")).alias("end"))
      .withColumn("monthDifference", datediff(col("end"), col("start")))
    .show(10, truncate = false)

  //altogether
  df.withColumn("Current date", current_date())
    .withColumn("Current time", current_timestamp())
    .withColumn("Day difference", datediff(col("Current date"), col("InvoiceDate")))
    .withColumn("Month difference", months_between(col("Current date"), col("InvoiceDate")))
    .show(2, truncate = false)

  //in SQL
  spark.sql(
    """
      |SELECT *,
      |current_date,
      |current_timestamp,
      |datediff(current_date, InvoiceDate) as day_difference,
      |months_between(current_date, InvoiceDate) as month_difference
      |FROM dfTable
      |""".stripMargin)
    .show(3, truncate = false)

}
