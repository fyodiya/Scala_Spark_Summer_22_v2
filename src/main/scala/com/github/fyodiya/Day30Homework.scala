package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.readDataWithView

object Day30Homework extends App {

  val spark = SparkUtilities.getOrCreateSpark("Joins playground")
  val dfRetail = readDataWithView(spark, "src/resources/retail-data/all/online-retail-dataset.csv")
  val dfCustomers = readDataWithView(spark, "src/resources/retail-data/customers.csv")

  dfRetail.createOrReplaceTempView("dfRetail")
  dfCustomers.createOrReplaceTempView("dfCustomers")

  //  inner join src/resources/retail-data/all/online-retail-dataset.csv
  //  with src/resources/retail-data/customers.csv
  //  on Customer ID in first matching Id in second

  val joinExpression = dfRetail.col("CustomerID") === dfCustomers.col("Id")
  dfRetail.join(dfCustomers, joinExpression).show()

  spark.sql(
    """
      |SELECT * FROM dfRetail JOIN dfCustomers
      |ON dfRetail.CustomerID = dfCustomers.Id""".stripMargin)
    .show()

}
