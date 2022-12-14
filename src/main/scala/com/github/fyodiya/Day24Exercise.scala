package com.github.fyodiya

import org.apache.spark.sql.functions.{col, expr, initcap, lpad, regexp_replace, rpad}

object Day24Exercise extends App {

  println("Day 24: Exercise with strings!")
  val spark = SparkUtilities.getOrCreateSpark("String Sandbox")

  //open up March 1st, of 2011 CSV
  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"
  val df = SparkUtilities.readDataWithView(spark, filePath)

  //Select Capitalized Description Column
  df.select(col("Description"),
    initcap(col("Description")))
    .show(5, truncate = false)

  //Select Padded country column with _ on both sides with 30 characters for country name total allowed
  df.select(
    col("Description"),
    col("Country"),
    //lpad(rpad(col("Country"), 22, "_"), 30, "_"), //doesn't work properly for all countries, number of __ on both sides isn't the same
    expr("lpad(rpad(Country, 15+int((CHAR_LENGTH(Country))/2), '_'), 30, '_') as ___Country___")
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
    .sample(withReplacement = false, 0.3)
    .show(50000000, truncate = false)

  //select Description column again with all occurrences of metal or wood replaced with material
  val materials = Seq("metal", "wood")
  val regexMaterials = materials.map(_.toUpperCase).mkString("|")

  df.select(
    regexp_replace(col("Description"), regexMaterials, "material").alias("materials clean"),
    col("Description"))
    .show(10,truncate = false)


  //so this description white metal lantern -> white material lantern
  //then show top 10 results of these 3 columns



}
