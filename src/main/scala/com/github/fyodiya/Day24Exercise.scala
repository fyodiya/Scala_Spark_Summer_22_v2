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
    col("Description"),
    col("Country"),
    lpad(rpad(col("Country"), 22, "_"), 30, "_")
      .as("_Country_")  )
    .show(5, truncate = false)
  //ideally there would be even number of padding on both sides:
  // _______LATVIA__________ (30 chars in total)

  spark.sql(
    """
      |SELECT Description,
      |Country,
      |lpad(Country, 22, '_'),
      |rpad(Country, 22, '_'),
      |lpad(rpad(Country, 15+int((CHAR_LENGTH(Country))/2), '_'), 30, '_') as ___Country___
      |FROM dfTable
      |""".stripMargin)
    .sample(false, 0.3)
    .show(50000000, false)

  //select Description column again with all occurrences of metal or wood replaced with material
  val materials = Seq("metal", "wood")
  val regexMaterials = materials.map(_.toUpperCase).mkString("|")

  df.select(
    regexp_replace(col("Description"), regexMaterials, "material").alias("materials clean"),
    col("Description"))
    .show(10,false)


  //so this description white metal lantern -> white material lantern
  //then show top 10 results of these 3 columns



}
