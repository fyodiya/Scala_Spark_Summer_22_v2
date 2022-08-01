package com.github.fyodiya

import org.apache.spark.sql.functions.{col, initcap, lit, lpad, regexp_replace, rpad}

object Day24Exercise extends App {

  println("Day 24: Exercise with strings!")
  val spark = SparkUtilities.getOrCreateSpark("String Sandbox")

  //open up March 1st, of 2011 CSV
  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"
  val df = SparkUtilities.readCSVWithView(spark, filePath)

  //Select Capitalized Description Column
  df.select(col("Description"),
    initcap(col("Description")))
    .show(5, truncate = false)

  //Select Padded country column with _ on both sides with 30 characters for country name total allowed
  df.select(
    col("Country"),
    lpad(rpad(lit("Latvia"), 12, "_"), 12, "_")
      .as("padded Country")  )
    .show(5, truncate = false)
  //ideally there would be even number of _______LATVIA__________ (30 total)

  //select Description column again with all occurrences of metal or wood replaced with material
  val materials = Seq("metal", "wood")
  val regexMaterials = materials.map(_.toUpperCase.mkString("|"))

  df.select(
    regexp_replace(col("Description"), regexMaterials, "MATERIAL").alias("material_clean"),
    col("Description"))
    .show(10, truncate = false)

  //so this description white metal lantern -> white material lantern
  //then show top 10 results of these 3 columns



}
