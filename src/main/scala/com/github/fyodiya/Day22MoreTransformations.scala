package com.github.fyodiya

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import com.github.fyodiya.Day21SelectExpressionsAndColumns.dfWithLongColName
import org.apache.spark.sql.functions.col

import scala.util.Random

object Day22MoreTransformations extends App {

  println("Chapter 5: Column and row operations!")
  val spark = SparkUtilities.getOrCreateSpark("BasicSpark")

  //this will set SQL sensitivity to be case sensitive:
  spark.conf.set("spark.sql.caseSensitive", true)
  //from now on the SQL queries will be case sensitive

  //there are many configuration settings you can set at runtime using the above syntax
  //https://spark.apache.org/docs/latest/configuration.html

  val flightPath = "src/resources/flight-data/json/2015-summary.json"

  //an automatic detection of schema
  val df = spark.read.format("json")
    .load(flightPath)

  df.show(5)

  //dropping columns
 // df.drop("ORIGIN_COUNTRY_NAME").columns

  //dropping multiple columns
  df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")
  //we could also do it by SELECT

  df.printSchema()

  //We can convert columns from one type to another by
  //casting the column from one type to another. For instance, let’s convert our count column from
  //an integer to a type Long

  //our count is already long
  //so will cast to integer so half the size of long
  //let's cast to double which is floating point just double precision of regular float
  val dfWith3Counts = df.withColumn("count2", col("count").cast("int"))
    .withColumn("count3", col("count").cast("double"))

  dfWith3Counts.show(5)
  dfWith3Counts.printSchema()

  //most often the cast would be from string to int, long or double
  //reason being that you want to perform some numeric calculation on that column

  //To filter rows, we create an expression that evaluates to true or false. You then filter out the rows
  //with an expression that is equal to false. The most common way to do this with DataFrames is to
  //create either an expression as a String or build an expression by using a set of column
  //manipulations. There are two methods to perform this operation: you can use where or filter
  //and they both will perform the same operation and accept the same argument types when used
  //with DataFrames. We will stick to WHERE because of its familiarity to SQL; however, filter is
  //valid as well.
  df.filter(col("count") < 2).show(2)
  df.where("count < 2").show(2)

  //prefer multiple chains
  df.where("count > 5")
    .where("count < 10")
    .show(5)

  // in Scala
  df.where(col("count") < 2)
    .where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
    .show(3)

  //Getting Unique Rows
  //A very common use case is to extract the unique or distinct values in a DataFrame. These values
  //can be in one or more columns. The way we do this is by using the distinct method on a
  //DataFrame, which allows us to deduplicate any rows that are in that DataFrame. For instance,
  //let’s get the unique origins in our dataset. This, of course, is a transformation that will return a
  //new DataFrame with only unique rows:

  val countUniqueFlights = df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
  println(s"Number of unique flights: ${countUniqueFlights}")

  //another way of getting the result
  println(df.select("ORIGIN_COUNTRY_NAME").distinct().count()) //should be 125 unique/distinct origin countries

  //Random Samples
  //Sometimes, you might just want to sample some random records from your DataFrame. You can
  //do this by using the sample method on a DataFrame, which makes it possible for you to specify
  //a fraction of rows to extract from a DataFrame and whether you’d like to sample with or without
  //replacement:

  //  val seed = 42 //this static seed should guarantee same sample each time
  val seed = Random.nextInt() //up to 4 billion different integers
  val withReplacement = false
  //if you set withReplacement to be true, that means you will be putting your row sampled back into the cookie jar
  //https://stackoverflow.com/questions/53689047/what-does-withreplacement-do-if-specified-for-sample-against-a-spark-dataframe
  //usually you do not want to draw the same ticket more than once
  val fraction = 0.1 //these 10% is just a rough estimate,
  //for smaller datasets such as ours it could be more or less
  //Note:
  //This does NOT guarantee to provide exactly the fraction of the count of the given Dataset.

  val dfSample = df.sample(withReplacement, fraction, seed)
  dfSample.show(5)
  println(s"We got ${dfSample.count()} samples.")

  // in Scala
  //we get Array of Dataset[Row] which is the same as Array[DataFrame]
  //splits 25 percent and 75 percent roughly, again - not exact fractions!
  val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)

  for ((dFrame, i) <- dataFrames.zipWithIndex) { //we could have used df or anything else instead of dFrame
    println(s"DataFrame No.$i has ${dFrame.count} rows.")
  }

  def getDataFrameStats(dFrames:Array[Dataset[Row]], df:DataFrame): Array[Long] = {
    dFrames.map(d => d.count() * 100 / df.count())
  }

  val dPercentages = getDataFrameStats(dataFrames, df)
  println("DataFrame percentages")
  dPercentages.foreach(println)

  //so now the proportion should be roughly 2/5 to the first dataframe and 3/5 to the 2nd
  //so randomSplit will normalize 2, 3 to 0.4, 0.6
  val dFrames23split = df.randomSplit(Array(2, 3), seed)
  getDataFrameStats(dFrames23split, df).foreach(println)

}
