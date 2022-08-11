package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.readDataWithView
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, count, expr, sum, to_date}

object Day29Rollups extends App {

  println("Chapter 7: Rollups!")
  val spark = SparkUtilities.getOrCreateSpark("Rollups playground")
  val filePath = "src/resources/retail-data/all/*.csv"
  val df = readDataWithView(spark, filePath)
  df.show(3)
  //df.describe().show() //takes about 20-30 seconds to get all stats

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
    "MM/d/yyyy H:mm"))
    .withColumn("total", expr("UnitPrice * Quantity"))
  println(s"We have ${dfWithDate.count} rows in dfWithDate")

  //we will need to drop null values to work with rollups

  val dfNoNull = dfWithDate.drop()
  println(s"We have ${dfNoNull.count} rows in dfNoNull")
  dfNoNull.createOrReplaceTempView("dfNoNull")

  //Rollups
  //Thus far, we’ve been looking at explicit groupings. When we set our grouping keys of multiple
  //columns, Spark looks at those as well as the actual combinations that are visible in the dataset. A
  //rollup is a multidimensional aggregation that performs a variety of group-by style calculations for us.
  //Let’s create a rollup that looks across time (with our new Date column) and space (with the
  //Country column) and creates a new DataFrame that includes the grand total over all dates, the
  //grand total for each date in the DataFrame, and the subtotal for each country on each date in the DataFrame

  val rolledUpDF = dfNoNull.rollup("Date", "Country")
    .agg(sum("Quantity"), count("Quantity").as("countQ"), sum("total").as("totalSales"))
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity", "countQ", "round(totalSales, 2)")
    .orderBy("Date", "Country")

  rolledUpDF.show()

  //  Now where you see the null values is where you’ll find the grand totals. A null in both rollup
  //    columns specifies the grand total across both of those columns:

  //this will show sales, quantity and total quantity sold for day by day
  rolledUpDF.where("Country IS NULL").show(50)

}
