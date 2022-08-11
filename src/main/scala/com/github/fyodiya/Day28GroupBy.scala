package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.readDataWithView
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{desc, expr}

object Day28GroupBy extends App {

  println("Chapter 7: Grouping expressions!")
  val spark = SparkUtilities.getOrCreateSpark("Sandbox for grouping by")
  val df = readDataWithView(spark, "src/resources/retail-data/by-day/2010-12-01.csv")


  //Thus far, we have performed only DataFrame-level aggregations. A more common task is to
  //perform calculations based on groups in the data. This is typically done on categorical data for
  //which we group our data on one column and perform some calculations on the other columns
  //that end up in that group.
  //The best way to explain this is to begin performing some groupings. The first will be a count,
  //just as we did before. We will group by each unique invoice number and get the count of items
  //on that invoice. Note that this returns another DataFrame and is lazily performed.
  //We do this grouping in two phases. First we specify the column(s) on which we would like to
  //group, and then we specify the aggregation(s). The first step returns a
  //RelationalGroupedDataset, and the second step returns a DataFrame.
  //As mentioned, we can specify any number of columns on which we want to group:

  df.groupBy("InvoiceNo", "CustomerId")
    .count()
    .orderBy("count")
    .show()

  //we get a unique combination of the columns above and counting the occurrences

  //in SQL
  spark.sql(
    """
      |SELECT InvoiceNo, CustomerId, count(*) AS cnt FROM dfTable
      |GROUP BY InvoiceNo, CustomerId
      |ORDER BY cnt DESC
      |""".stripMargin)
    .show(20)

  df.groupBy("InvoiceNo")
    .count()
    .orderBy(desc("count"))
    .show(20)

  //Grouping with Expressions
  //As we saw earlier, counting is a bit of a special case because it exists as a method. For this,
  //usually we prefer to use the count function. Rather than passing that function as an expression
  //into a select statement, we specify it as within agg. This makes it possible for you to pass-in
  //arbitrary expressions that just need to have some aggregation specified. You can even do things
  //like alias a column after transforming it for later use in your data flow:

  df.groupBy("InvoiceNo")
    .agg(
      functions.count("Quantity").alias("quan"),
      expr("count(Quantity)"))
    .show()
  //the same results for both expressions

  df.withColumn("total", expr("round(Quantity * UnitPrice, 4)"))
    .groupBy("InvoiceNo")
    .agg(expr("count(Quantity) as Qcount"),
      expr("count(total) as Tcount"),
      expr("sum(total) as totalSales"),
      expr("avg(total) as avgSales"),
      expr("min(total) as lowestSale"),
      expr("max(total) as maxSale"))
    .orderBy(desc("totalSales"))
    .show(10, truncate = false)

  df.withColumn("total", expr("round(Quantity * UnitPrice, 4)"))
    .groupBy("Country")
    .agg(expr("count(Quantity) as Qcount"),
      expr("count(total) as Tcount"),
      expr("sum(total) as totalSales"),
      expr("avg(total) as avgSales"),
      expr("min(total) as lowestSale"),
      expr("max(total) as maxSale"),
      expr("round(std(total),2) as saleSTD"),
      expr("first(CustomerID) as firstCustomer"),
      expr("last(CustomerID) as lastCustomer"),
      expr("count(Distinct CustomerID) as distinctCustomers"), //count distinct customers in the same function
      expr("count(CustomerId) as customersPerCountry"))
    .orderBy(desc("totalSales"))
    .show(10, truncate = false)

}
