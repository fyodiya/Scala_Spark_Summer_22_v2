package com.github.fyodiya

import org.apache.spark.sql.functions.{avg, collect_list, collect_set, corr, covar_pop, covar_samp, expr, kurtosis, lit, mean, skewness, stddev_pop, stddev_samp, var_pop, var_samp}

object Day28Exercise extends App {

  println("Chapter 7: Homework on statistical operations")
  val spark = SparkUtilities.getOrCreateSpark("Sandbox for statistical operations")
  //load March 8th of 2011 CSV
  val homeworkDF = SparkUtilities.readDataWithView(spark, filePath = "src/resources/retail-data/by-day/2011-03-08.csv")

  //lets show avg, variance, std, skew, kurtosis, correlation and population covariance
  homeworkDF
    .select(
     avg("Quantity").alias("avg_purchases"),
      var_pop("Quantity").alias("var_pop"),
      //      var_samp("Quantity").alias("var_samp"),
      stddev_pop("Quantity").alias("standard_deviation"),
      skewness("Quantity").alias("skewness"),
      kurtosis("Quantity").alias("kurtosis"),
      corr("InvoiceNo", "Quantity").alias("correlation_InvoiceNo_and_Quantity"),
      covar_samp("InvoiceNo", "Quantity"),
      covar_pop("InvoiceNo", "Quantity").alias("cover_pop"))
    .selectExpr(
      "avg_purchases", "standard_deviation", "skewness", "kurtosis", "correlation_InvoiceNo_and_Quantity", "cover_pop")
    .show()


  //TODO transform unique Countries for that day into a regular Scala Array of strings
  val distCountries = homeworkDF.agg(collect_set("Country"))
  //  val countryArray = distCountries.collectAsList().toArray().map(_.toString)
  //  countryArray.foreach(println)
  //
  //you could use SQL distinct of course - you do not have to use collect_set, but you can :)

}
