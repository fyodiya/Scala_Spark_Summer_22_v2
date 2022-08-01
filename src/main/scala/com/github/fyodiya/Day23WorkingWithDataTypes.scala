package com.github.fyodiya

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{bround, col, corr, count, countDistinct, expr, lit, max, mean, min, monotonically_increasing_id, ntile, pow, round, stddev_pop}

object Day23WorkingWithDataTypes extends App {

  println("Continuing work with different data types.")
  val spark = SparkUtilities.getOrCreateSpark("Spark Sandbox")
  //  spark.conf.set("spark.sql.caseSensitive", true)

  val filePath = "src/resources/retail-data/by-day/2011-12-01.csv"
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)
  df.printSchema()

  df.createOrReplaceTempView("dfTable")

  //you can specify Boolean expressions with multiple parts when you use and
  //or or. In Spark, you should always chain together and filters as a sequential filter.
  //The reason for this is that even if Boolean statements are expressed serially (one after the other),
  //Spark will flatten all of these filters into one statement and perform the filter at the same time,
  //creating the and statement for us. Although you can specify your statements explicitly by using
  //and if you like, they’re often easier to understand and to read if you specify them serially. or
  //statements need to be specified in the same statement:

  val priceFilter = col("UnitPrice") > 600 //filter is a column type
  val descripFilter = col("Description").contains("POSTAGE") //again - column type

  //here we use pre-existing ordering
  df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter))
  .show()

  spark.sql("SELECT * FROM dfTable " +
    "WHERE StockCode IN ('DOT') AND (UnitPrice > 600 OR Description LIKE '%POSTAGE%')")
    .show()

  //Boolean expressions are not just reserved to filters. To filter a DataFrame, you can also just
  //specify a Boolean column:
  val DOTCodeFilter = col("StockCode") === "DOT"
  //we add a new Boolean colum which shows whether StockCode is named DOT
  df.withColumn("stockCodeDOT", DOTCodeFilter).show(10)

  df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
    .where("isExpensive")
    .select("unitPrice", "isExpensive")
    .show(5)

  //WARNING
  //One “gotcha” that can come up is if you’re working with null data when creating Boolean expressions.
  //If there is a null in your data, you’ll need to treat things a bit differently. Here’s how you can ensure
  //that you perform a null-safe equivalence test:
  //if description might have null/ does not exist, we need to use this eqNullSafe method
  df.where(col("Description").eqNullSafe("hello")).show()

//Working with Numbers
  //When working with big data, the second most common task you will do after filtering things is
  //counting things. For the most part, we simply need to express our computation, and that should
  //be valid assuming that we’re working with numerical data types.
  //To fabricate a contrived example, let’s imagine that we found out that we mis-recorded the
  //quantity in our retail dataset and the true quantity is equal to (the current quantity * the unit
  //price) + 5. This will introduce our first numerical function as well as the pow function that raises
  //a column to the expressed power:

  val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
  df.select(col("CustomerId"),
    col("Quantity"),
    col("UnitPrice"),
    fabricatedQuantity.alias("realQuantity"))
    .show(2)

  df.selectExpr(
    "CustomerId",
    "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)

  //Another common numerical task is rounding. If you’d like to just round to a whole number,
  //oftentimes you can cast the value to an integer and that will work just fine. However, Spark also
  //has more detailed functions for performing this explicitly and to a certain level of precision. In
  //the following example, we round to one decimal place:
  df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)

//  By default, the round function rounds up if you’re exactly in between two numbers. You can
//    round down by using the bround:
  df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

  //Another numerical task is to compute the correlation of two columns. For example, we can see
  //the Pearson correlation coefficient for two columns to see if cheaper things are typically bought
  //in greater quantities. We can do this through a function as well as through the DataFrame
  //statistic methods:
  var corrCoefficient = df.stat.corr("Quantity", "UnitPrice")
  println(s"Pearson correlation coefficient for Quantity and UnitPrice is $corrCoefficient")
  df.select(corr("Quantity", "UnitPrice")).show()

  //Another common task is to compute summary statistics for a column or set of columns. We can
  //use the describe method to achieve exactly this. This will take all numeric columns and
  //calculate the count, mean, standard deviation, min, and max. You should use this primarily for
  //viewing in the console because the schema might change in the future:
  df.describe().show()

  //If you need these exact numbers, you can also perform this as an aggregation yourself by
  //importing the functions and applying them to the columns that you need:
  df.select(mean("Quantity"), mean("UnitPrice"), max("UnitPrice")).show()

  //There are a number of statistical functions available in the StatFunctions Package (accessible
  //using stat as we see in the code block below). These are DataFrame methods that you can use
  //to calculate a variety of different things. For instance, you can calculate either exact or
  //approximate quantiles of your data using the approxQuantile method:
  val colName = "UnitPrice"
  val quantileProbs = Array(0.1, 0.4, 0.5, 0.6, 0.9, 0.99) //checking different quantiles
  val relError = 0.05
  val quantilePrices = df.stat.approxQuantile("UnitPrice", quantileProbs, relError)
//  df.stat.approxQuantile("UnitPrice", quantileProbs, relError) // 2.51

  for ((prob, price) <- quantileProbs zip quantilePrices) {
    println(s"Quantile ${prob} - price ${price}")
  }

  //what happens if you change relError to 0.1?
  //quantiles change!

  def getQuantiles(df: DataFrame, colName:String, quantileProbs:Array[Double]=Array(0.25,0.5,0.75,0.99), relError:Double=0.05):Array[Double] = {
    df.stat.approxQuantile(colName, quantileProbs, relError)
  }

  def printQuantiles(df: DataFrame, colName:String, quantileProbs:Array[Double]=Array(0.25,0.5,0.75,0.99), relError:Double=0.05): Unit = {
    val quantiles = getQuantiles(df, colName, quantileProbs, relError)
    println(s"For column $colName")
    for ((prob, cutPoint) <- quantileProbs zip quantiles) {
      println(s"Quantile ${prob} so aprox ${Math.round(prob*100)}% of data is covered - cutPoint ${cutPoint}")
    }
  }

  val deciles = (1 to 10).map(n => n.toDouble/10).toArray
  printQuantiles(df, "UnitPrice", deciles)

  //ventiles 20-quantiles
  val ventiles = (1 to 20).map(n => n.toDouble/20).toArray
  printQuantiles(df, "UnitPrice", ventiles)

//  You also can use this to see a cross-tabulation or frequent item pairs (be careful, this output will
//    be large and is omitted for this reason):
  df.stat.crosstab("StockCode", "Quantity").show()

  //As a last note, we can also add a unique ID to each row by using the function
  //monotonically_increasing_id. This function generates a unique value for each row, starting
  //with 0:
  df.select(monotonically_increasing_id()).show(2)

  //we might want to have some distinct counts for ALL columns
  //https://stackoverflow.com/questions/40888946/spark-dataframe-count-distinct-values-of-every-column

  df.groupBy("Country").count().show()
  df.agg(countDistinct("Country")).show()

  for (col <- df.columns) {
    //    val count = df.agg(countDistinct(col))
    //this would get you breakdown by distinct value
    //    val count = df.groupBy(col).count().collect()
    val count = df.groupBy(col).count().count() //the first Count is aggregation second count is row count in our results
    println(s"Column $col has $count distinct values")
  }

  //so here crosstab would be huge here we could do something like
  //quantile breakdown by country :)
  //so first we would need to get quantile rank for each row
  //  val windowSpec  = Window.partitionBy("UnitPrice").orderBy("UnitPrice")
  //here we do not need to partition we want rank over ALL of the rows
  val windowSpec  = Window.partitionBy().orderBy("UnitPrice") //TODO check if window applies to whole DF
  //in chapter 7

  df.select(col("Description"), col("UnitPrice"),
    ntile(4).over(windowSpec).alias("quantile_rank") //we are trying to get rank 1 to 4 for each row
  ).show()
  //TODO double check syntax for quartile ranking

  //As a last note, we can also add a unique ID to each row by using the function
  //monotonically_increasing_id. This function generates a unique value for each row, starting
  //with 0:
  df.select(monotonically_increasing_id()).show(5)

}
