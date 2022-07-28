package com.github.fyodiya

case class DFStats(number: Int, rowCount: Long, percentage: Long)

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col
import scala.util.Random

object Day22Exercise extends App {

  println("Filtering and splitting DataFrames!")
  val spark = SparkUtilities.getOrCreateSpark("Spark_exercise")

  //open up 2014-summary.json file
  val flightPath = "src/resources/flight-data/json/2014-summary.json"
  val df = spark.read.format("json")
    .load(flightPath)

  //Task 1 - Filter ONLY flights FROM US that happened more than 10 times
  df.where(col("ORIGIN_COUNTRY_NAME") === "United States")
    .where(col("count") > 10)
    .show()

  // val USdf = df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME", "count")
  val USdf = df.where(col("DEST_COUNTRY_NAME") =!= "United States")
    .where(col("count")>10)
    .distinct()
  USdf.show()

  //a fixed seed
  //val seed = 5

  //Task 2 - I want a random sample from all 2014 of roughly 30 percent, you can use a fixed seed
  val seed = Random.nextInt()
  val withReplacement = false //we don't place our samples back into the sample pool
  val fraction = 0.3

  val sample = df.sample(withReplacement, fraction, seed)
  sample.show()
  //subtask I want to see the actual row count
  println(s"We got ${sample.count()} samples")
  println()

  //Task 3 - I want a split of full 2014 dataframe into 3 Dataframes
  //with the following proportions: 2, 9, 5
  val dataFrames = df.randomSplit(Array(2,9,5), seed)

  def getDataFrameStats(dFrames:Array[Dataset[Row]], df:DataFrame): Array[Long] = {
    dFrames.map(d => d.count() * 100 / df.count())
  }
  getDataFrameStats(dataFrames, df).foreach(println)

  //subtask - I want to see the row count for these dataframes and percentages
  for ((dFrame, i) <- dataFrames.zipWithIndex) {
    println(s"$i dataframe consists of ${dFrame.count} rows")
  }

}
