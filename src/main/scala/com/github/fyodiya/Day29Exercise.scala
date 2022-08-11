package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.readDataWithView
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, rank, max, min, desc, to_date}

object Day29Exercise extends App {

  val spark = SparkUtilities.getOrCreateSpark("Homework")
  val df = readDataWithView(spark, "src/resources/retail-data/by-day/2010-12-01.csv")

  //create WindowSpec which partitions by StockCode and date, ordered by Price

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
//    "MM/d/yyyy H:mm"))
    "M/D/y H:mm"))
  dfWithDate.createOrReplaceTempView("dfWithDate")

  //with rows unbounded preceding and current row
  val windowSpec = Window
    .partitionBy("StockCode", "date")
    .orderBy(col("UnitPrice").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val maxPrice = max(col("UnitPrice")).over(windowSpec)
  val minPrice = min(col("UnitPrice")).over(windowSpec)

  //create max min dense rank and rank for the price over the newly created WindowSpec
  val priceDenseRank = dense_rank().over(windowSpec)
  val priceRank = rank().over(windowSpec)

  //show top 40 results ordered in descending order by StockCode and price
  //show max, min, dense rank and rank for every row as well using our newly created columns(min, max, dense rank and rank)

  dfWithDate.where("CustomerId IS NOT NULL")
    .orderBy(desc("StockCode"), col("UnitPrice"))
    .select(
      col("CustomerId"),
      col("date"),
      col("Quantity"),
      col("UnitPrice"),
      priceRank.alias("priceRank"),
      priceDenseRank.alias("priceDenseRank"),
      maxPrice.alias("maxPrice"),
      minPrice.alias("minPrice"))
    .show(40, truncate = false)


  //you can use spark API functions
  //or you can use spark sql

}
