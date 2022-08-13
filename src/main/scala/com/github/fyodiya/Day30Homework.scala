package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.readDataWithView
import org.apache.spark.sql.functions.{desc, grouping_id, max, min, sum}

object Day30Homework extends App {

  val spark = SparkUtilities.getOrCreateSpark("Joins playground")
  val dfRetail = readDataWithView(spark, "src/resources/retail-data/all/online-retail-dataset.csv", viewName = "dfRetail")
  val dfCustomers = readDataWithView(spark, "src/resources/retail-data/customers.csv", viewName = "dfCustomers")

//  dfRetail.createOrReplaceTempView("dfRetail")
//  dfCustomers.createOrReplaceTempView("dfCustomers")

  //  inner join src/resources/retail-data/all/online-retail-dataset.csv
  //  with src/resources/retail-data/customers.csv
  //  on Customer ID in first matching Id in second

  val joinExpression = dfRetail.col("CustomerID") === dfCustomers.col("Id")
  dfRetail.join(dfCustomers, joinExpression).show()

  spark.sql(
    """
      |SELECT * FROM dfRetail JOIN dfCustomers
      |ON dfRetail.CustomerID = dfCustomers.Id
      |ORDER BY ' LastName' DESC""".stripMargin)
    .show(50, truncate = false)



  val realPurchases = dfRetail.join(dfCustomers, joinExpression)
  realPurchases.show(30)
  realPurchases.groupBy(" LastName").sum("Quantity")

//  realPurchases.cube(" LastName", "InvoiceNo").sum("Quantity")
//    .show()

  //same as null null in cube - grouping id == 3
  realPurchases.select(sum("Quantity"), min("Quantity"), max("Quantity"))
    .show()

  realPurchases.cube(" LastName", "InvoiceNo")
    .agg(grouping_id(), sum("Quantity"), min("Quantity"), max("Quantity"))
    .orderBy(desc("grouping_id()")) //level 3 is everything which is in the table for these 2 groups
    .show()

  realPurchases.groupBy(" LastName").sum("Quantity").show()

  realPurchases.groupBy("StockCode", "Description") //this won't generate new rows since StockCode has unique No
    .pivot(" LastName")
    .sum("Quantity")
    .show(20, truncate = false)


}
