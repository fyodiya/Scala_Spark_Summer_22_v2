package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.readDataWithView
import org.apache.spark.sql.functions.{col, count, expr, grouping_id, sum, to_date}

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

  //Cube
  //A cube takes the rollup to a level deeper. Rather than treating elements hierarchically, a cube
  //does the same thing across all dimensions. This means that it won’t just go by date over the
  //entire time period, but also the country. To pose this as a question again, can you make a table
  //that includes the following?
  //The total across all dates and countries
  //The total for each date across all countries
  //The total for each country on each date
  //The total for each country across all dates
  //The method call is quite similar, but instead of calling rollup, we call cube:

  //cubes are stats for everything involving these two columns
  dfNoNull.cube("Date", "Country")
    .agg(grouping_id(),sum(col("Quantity")), sum("total"))
    .select("Date", "Country", "grouping_id()", "sum(Quantity)", "sum(total)").orderBy("Date")
    .show(50)

  //This is a quick and easily accessible summary of nearly all of the information in our table, and
  //it’s a great way to create a quick summary table that others can use later on.

  //Grouping Metadata
  //Sometimes when using cubes and rollups, you want to be able to query the aggregation levels so
  //that you can easily filter them down accordingly. We can do this by using the grouping_id,
  //which gives us a column specifying the level of aggregation that we have in our result set. The
  //query in the example that follows returns four distinct grouping IDs:

  dfNoNull.cube("customerId", "stockCode")
    .agg(grouping_id(), sum("Quantity"), sum("total"))
    .orderBy(expr("grouping_id()").desc)
    .show()

  //Pivot
  //Pivots make it possible for you to convert a row into a column. For example, in our current data
  //we have a Country column. With a pivot, we can aggregate according to some function for each
  //of those given countries and display them in an easy-to-query way:

  val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()

  //his DataFrame will now have a column for every combination of country, numeric variable,
  //and a column specifying the date. For example, for USA we have the following columns:
  //USA_sum(Quantity), USA_sum(UnitPrice), USA_sum(CustomerID). This represents one for
  //each numeric column in our dataset (because we just performed an aggregation over all of them).
  //Here’s an example query and result from this data:

  pivoted.where("date > '2011-12-05'").select("date" ,"`USA_sum(Quantity)`").show()

  //Now all of the columns can be calculated with single groupings, but the value of a pivot comes
  //down to how you would like to explore the data. It can be useful, if you have low enough
  //cardinality in a certain column to transform it into columns so that users can see the schema and
  //immediately know what to query for.


  dfWithDate.show(5, truncate = false)
  dfWithDate
    .select("Country")
    .distinct()
    .orderBy("Country")
    .show(30)

  val pivotedToo = dfWithDate.groupBy("date").pivot("Country").sum()
  pivotedToo.show(20, truncate = false)
  //many columns equal to distinct country count * numeric columns in original df

}
