package com.github.fyodiya

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{asc, col, desc, expr}
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

  //DataFrames are immutable. This means users cannot
  //append to DataFrames because that would be changing it. To append to a DataFrame, you must
  //union the original DataFrame along with the new DataFrame. This just concatenates the two
  //DataFrames. To union two DataFrames, you must be sure that they have the same schema and
  //number of columns; otherwise, the union will fail.

  //Unions are currently performed based on location, not on the schema. This means that columns will
  //not automatically line up the way you think they might.

  val unionFirstTwo = dataFrames.head.union(dataFrames(1))
  unionFirstTwo.show(5)
  println(s"The size of the union of 2 dataframes is: ${unionFirstTwo.count}")

  val schema = df.schema //we copied an original dataframe schema
  val newRows = Seq(
    Row("New Country", "Other Country", 5L),
    Row("New Country 2", "Other Country 3", 1L)
  )
  val parallelizedRows = spark.sparkContext.parallelize(newRows)
  val newDF = spark.createDataFrame(parallelizedRows, schema)
  df.printSchema()

  df.union(newDF)
    .where("count = 1")
    .where("ORIGIN_COUNTRY_NAME != 'United States'")
    .show() // get all of them and we'll see our new rows at the end

  //Sorting Rows
  //When we sort the values in a DataFrame, we always want to sort with either the largest or
  //smallest values at the top of a DataFrame. There are two equivalent operations to do this sort
  //and orderBy that work the exact same way. They accept both column expressions and strings as
  //well as multiple columns.
  // The default is to sort in ascending order

  //different approaches to sorting
  df.sort("count").show(5)
  //when there are 2 columns, the 2nd one is the tie-breaker
  df.sort(desc("count")).show(5)
  //since DESC counts are different straight from the beginning,  only the tiny counts (1, 2, 3 would have tiebreakes
  //you'd expect all 3 of these to be exactly the same

  df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
  df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5) //better to skip this one

  //To more explicitly specify sort direction, you need to use the asc and desc functions if operating
  //on a column. These allow you to specify the order in which a given column should be sorted:
  df.orderBy(expr("count desc")).show(2)
  df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)
  //you can keep adding more tiebreakers if the query/data is complicated

//Day 23, finishing up on limit and collect & coalesce

  //Limit
  //Oftentimes, you might want to restrict what you extract from a DataFrame; for example, you
  //might want just the top ten of some DataFrame. You can do this by using the limit method:
  df.limit(5).show() //only 5 results will be shown

  //remember, that after creating a temporary view operations can be performed by using SQL syntax
  df.createTempView("dfTable")
  spark.sql("SELECT * FROM dfTable ORDER BY count DESC LIMIT 3").show(5) //only 3 result will be shown!

  //Repartition and Coalesce
  //Another important optimization opportunity is to partition the data according to some frequently
  //filtered columns, which control the physical layout of data across the cluster including the
  //partitioning scheme and the number of partitions.
  //Repartition will incur a full shuffle of the data, regardless of whether one is necessary. This
  //means that you should typically only repartition when the future number of partitions is greater
  //than your current number of partitions or when you are looking to partition by a set of columns:

  println(s"We have partition count: ${df.rdd.getNumPartitions}")

  //If you know that you’re going to be filtering by a certain column often, it can be worth
  //repartitioning based on that column:
  val newDFSinglePart = df.repartition(5, col("DEST_COUNTRY_NAME"))
  println(s"We have partition count: ${df.rdd.getNumPartitions}")
  println(s"We have partition count: ${newDFSinglePart.rdd.getNumPartitions}")

  //You can optionally specify the number of partitions you would like, too:
  df.repartition(5, col("DEST_COUNTRY_NAME"))

  //Coalesce, on the other hand, will not incur a full shuffle and will try to combine partitions. This
  //operation will shuffle your data into five partitions based on the destination country name, and
  //then coalesce them (without a full shuffle):
  val dfCoalesced2 = df.repartition(10, col("DEST_COUNTRY_NAME")).coalesce(2)
  println(s"We have partition count: ${df.rdd.getNumPartitions}") //should be still 1
  println(s"We have partition count: ${dfCoalesced2.rdd.getNumPartitions}") //should be 2
  //again with a single machine there is not much point in partitions this is meant for actual deployments over multiple machines

//Collecting Rows to the Driver
  //As discussed in previous chapters, Spark maintains the state of the cluster in the driver. There are
  //times when you’ll want to collect some of your data to the driver in order to manipulate it on
  //your local machine.
  //Thus far, we did not explicitly define this operation. However, we used several different methods
  //for doing so that are effectively all the same. collect gets all data from the entire DataFrame,
  //take selects the first N rows, and show prints out a number of rows nicely.
  val collectDF = df.limit(10) //we don;t have the data locally, this is just an instruction
  val arrRow5 = collectDF.take(5) // take works with an Integer count
  collectDF.show() // this prints it out nicely
  collectDF.show(5, false) //keeps long strings inside cells for show

  val arrRow10 = collectDF.collect() //Returns an array that contains all rows in this Dataset.
  // Running collect requires moving all the data into the application's driver process,
  // and  by doing so on a very large dataset it can crash the driver process with OutOfMemoryError.

  //now that we have data locally we can do whatever we want using standard Scala code
  arrRow5.foreach(println)
  println("All 10")
  arrRow10.foreach(println)

}
