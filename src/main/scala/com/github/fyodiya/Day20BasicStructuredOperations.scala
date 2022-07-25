package com.github.fyodiya

import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.types.Metadata

object Day20BasicStructuredOperations extends App {

  println("Chapter 5: Basic structured operations!")
  val spark = SparkUtilities.getOrCreateSpark("BasicSpark")

  val flightPath = "src/resources/flight-data/json/2015-summary.json"

  val df = spark.read.format("json")
    .load(flightPath)

//  df.show(5)
//  println(df.schema)
  df.printSchema() //another option to do the same

//A schema is a StructType made up of a number of fields, StructFields, that have a name,
//type, a Boolean flag which specifies whether that column can contain missing or null values,
//and, finally, users can optionally specify associated metadata with that column. The metadata is a
//way of storing information about this column (Spark uses this in its machine learning library).
//Schemas can contain other StructTypes (Spark’s complex types). We will see this in Chapter 6
//when we discuss working with complex types. If the types in the data (at runtime) do not match
//the schema, Spark will throw an error. The example that follows shows how to create and
//enforce a specific schema on a DataFrame.

  val myManualSchema = StructType(Array(
    StructField("DEST_COUNTRY_NAME", StringType, true),
    StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    StructField("count", LongType, false,
      Metadata.fromJson("{\"hello\":\"world\"}"))
  ))

  col("someColumnName")
//  column("someColumnName") //does the same thing

  //An expression, created via the expr function, is just a DataFrame column
  //reference. In the simplest case, expr("someCol") is equivalent to col("someCol")

  //expr("someCol - 5") is the same transformation as performing col("someCol") - 5, or even
  //expr("someCol") - 5. That’s because Spark compiles these to a logical tree specifying the
  //order of operations.

  //you can write your expressions as DataFrame code or as SQL
  //expressions and get the exact same performance characteristics

  //accessing a dataframe's columns
//  spark.read.format("json").load("/data/flight-data/json/2015-summary.json")
//    .columns

  println("Columns in our dataframe are:")
  println(df.columns.mkString(", "))

//rows and records in DFs are the same thing

  //accessing our first row
  val firstRow = df.first()
  println(firstRow)

  val lastRow = df.tail(1)(0) //returns an array
  println(lastRow)

  //before saving/writing we need to create a partition
  df.coalesce(1)
    .write
    .option("header", "true")
    .option("sep", ",")
    .mode("overwrite")
    .csv("src/resources/csv/flight_summary_2015.csv")

  //coalesce(1) was needed because otherwise Spark would have saved our data in 5 different csv files
  //(because we have 5 partitions from our configuration)

  //TRANSFORMATION



}
