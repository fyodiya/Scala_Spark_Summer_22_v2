package com.github.fyodiya

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, udf}

object Day27Exercises extends App {

  val spark = SparkUtilities.getOrCreateSpark("Exercises_playground")

  //TODO create a UDF which converts Fahrenheit to Celsius
  //run it - create a DF with col(tempF) from -40 to 120
  //register your UDF
  //show both created views, show columns starting with F 90 (included) and ending with F 110 (included)
  //use your UDF to create a temp column with the actual temperature

  //double incoming and double as a return

  val tempDF = spark.range(-40, 120).toDF("temperature_Fahrenheit")
  tempDF.printSchema()
//  tempDF.show()

  def fahrenheitToCelsius(tempF: Double):Double = ((tempF - 32) * 5 / 9).round
  val tempUDF = udf(fahrenheitToCelsius(_:Double):Double)

  spark.udf.register("temperatures", fahrenheitToCelsius(_:Double): Double)

  tempDF
    .withColumn("Celsius", tempUDF(col("temperature_Fahrenheit")))
    .select("*")
    .filter("temperature_Fahrenheit >= 90 AND temperature_Fahrenheit <= 110")
    .show()


  //df.filter(df("state") === "OH" && df("gender") === "M")


  //simple task - find count, distinct count, approximate count (with default rsd)
  //for customer ID AND unit price columns AND invoice numbers
  //count should be the same for all these (because that's the number of rows)


  val filePath = "src/resources/retail-data/all/online-retail-dataset.csv"
  val df = SparkUtilities.readDataWithView(spark, filePath)

//  println(df.count(), "rows")

  df
    .select("CustomerID", "InvoiceNo", "UnitPrice")
     .select(functions.count("CustomerID"))
     .show()

  df
    .select("CustomerID", "InvoiceNo", "UnitPrice")
    .select(functions.countDistinct("CustomerID"))
    .show()

  df
    .select("CustomerID", "InvoiceNo", "UnitPrice")
    .select(functions.approx_count_distinct("CustomerID"))
    .show()

}
