package com.github.fyodiya

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{approx_count_distinct, countDistinct, first, last}


object Aggregations extends App {

  println("Chapter 7: Aggregations!")
  val spark = SparkUtilities.getOrCreateSpark("Playground for aggregations")

//Aggregating is the act of collecting something together and is a cornerstone of big data analytics.
  //In an aggregation, you will specify a key or grouping and an aggregation function that specifies
  //how you should transform one or more columns. This function must produce one result for each
  //group, given multiple input values. Spark’s aggregation capabilities are sophisticated and mature,
  //with a variety of different use cases and possibilities. In general, you use aggregations to
  //summarize numerical data usually by means of some grouping. This might be a summation, a
  //product, or simple counting. Also, with Spark you can aggregate any kind of value into an array,
  //list, or map

  //In addition to working with any type of values, Spark also allows us to create the following
  //groupings types:
  //The simplest grouping is to just summarize a complete DataFrame by performing an
  //aggregation in a select statement.
  //A “group by” allows you to specify one or more keys as well as one or more
  //aggregation functions to transform the value columns.
  //A “window” gives you the ability to specify one or more keys as well as one or more
  //aggregation functions to transform the value columns. However, the rows input to the
  //function are somehow related to the current row.
  //A “grouping set,” which you can use to aggregate at multiple different levels. Grouping
  //sets are available as a primitive in SQL and via rollups and cubes in DataFrames.
  //A “rollup” makes it possible for you to specify one or more keys as well as one or more
  //aggregation functions to transform the value columns, which will be summarized
  //hierarchically.
  //A “cube” allows you to specify one or more keys as well as one or more aggregation
  //functions to transform the value columns, which will be summarized across all
  //combinations of columns.

  //Each grouping returns a RelationalGroupedDataset on which we specify our aggregations.

  val filePath = "src/resources/retail-data/all/online-retail-dataset.csv"
  val df = SparkUtilities.readDataWithView(spark, filePath)

//  df.printSchema() - it has already been done by our util function
  df.show(5, truncate = false)

//As mentioned, basic aggregations apply to an entire DataFrame. The simplest example is the
  //count method:
  println(df.count(), "rows")

  //If you’ve been reading this book chapter by chapter, you know that count is actually an action as
  //opposed to a transformation, and so it returns immediately. You can use count to get an idea of
  //the total size of your dataset but another common pattern is to use it to cache an entire
  //DataFrame in memory, just like we did in this example.
  //Now, this method is a bit of an outlier because it exists as a method (in this case) as opposed to a
  //function and is eagerly evaluated instead of a lazy transformation. In the next section, we will see
  //count used as a lazy function, as well

  //most things in Spark are lazy, they are being done already when it's absolutely needed

  //All aggregations are available as functions

  //count
  //The first function worth going over is count, except in this example it will perform as a
  //transformation instead of an action. In this case, we can do one of two things: specify a specific
  //column to count, or all the columns by using count(*) or count(1) to represent that we want to
  //count every row as the literal one, as shown in this example:

  df.select(functions.count("StockCode")).show() // 541909

  //in SQL
  spark.sql(
    """
      |SELECT COUNT(*) FROM dfTable
      |""".stripMargin
  )

  //WARNING
  //There are a number of gotchas when it comes to null values and counting. For instance, when
  //performing a count(*), Spark will count null values (including rows containing all nulls). However,
  //when counting an individual column, Spark will not count the null values.

  //countDistinct
  //Sometimes, the total number is not relevant; rather, it’s the number of unique groups that you
  //want. To get this number, you can use the countDistinct function. This is a bit more relevant
  //for individual columns:

  df.select(countDistinct("StockCode")).show() // 4070

  //approx_count_distinct
  //Often, we find ourselves working with large datasets and the exact distinct count is irrelevant.
  //There are times when an approximation to a certain degree of accuracy will work just fine, and
  //for that, you can use the approx_count_distinct function:

  df.select(approx_count_distinct("StockCode", 0.1)).show() // 3364
  df.select(approx_count_distinct("StockCode")).show()
  df.select(approx_count_distinct("StockCode", 0.01)).show()

  //in SQL
  spark.sql(
    """
      |SELECT approx_count_distinct(StockCode, 0.1) FROM DFTABLE
      |""".stripMargin)
    .show(5, truncate = false)

  //You will notice that approx_count_distinct took another parameter with which you can
  //specify the maximum estimation error allowed. In this case, we specified a rather large error and
  //thus receive an answer that is quite far off but does complete more quickly than countDistinct.
  //You will see much greater performance gains with larger datasets.
  //first and last

  //You can get the first and last values from a DataFrame by using these two obviously named
  //functions. This will be based on the rows in the DataFrame, not on the values in the DataFrame:
  df.select(first("StockCode"), last("StockCode")).show()

  //in SQL
  spark.sql(
    """
      |SELECT first(StockCode), last(StockCode) FROM dfTable
      |""".stripMargin)
    .show(10, truncate = false)

  //TODO simple task - find count, distinct count, approximate count (with default rsd)
  //for customer ID AND unit price columns AND invoice numbers

}
