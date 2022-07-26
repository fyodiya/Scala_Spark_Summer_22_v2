package com.github.fyodiya

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Day21RowsAndDFTransformations extends App {

  println("Rows and dataframe transformations!")
  val spark = SparkUtilities.getOrCreateSpark("BasicSpark")

  val flightPath = "src/resources/flight-data/json/2015-summary.json"

  //creating rows
  //You can create rows by manually instantiating a Row object with the values that belong in each
  //column. It’s important to note that only DataFrames have schemas. Rows themselves do not have
  //schemas. This means that if you create a Row manually, you must specify the values in the same
  //order as the schema of the DataFrame to which they might be appended

  val myRow = Row("Hello!", null, 345, false, 3.14)
  println(myRow(0)) //any type
  println(myRow(0).asInstanceOf[String]) //String
  myRow.getString(0) //String
  val myGreeting = myRow.getString(0)
  myRow.getInt(2) //Int
  val myDouble = myRow.getInt(2).toDouble //we cast our Int as a double
                                          //here we use regular Scala method
  println(myDouble)
  val myPi = myRow.getDouble(4)
  println(myPi)

  println(myRow.schema) //we can print out the schema for a single row despite not having a special method like printschema


  //DATAFRAME TRANSFORMATIONS
  //some fundamentals are:
  //We can add rows or columns
  //We can remove rows or columns
  //We can transform a row into a column (or vice versa)
  //We can change the order of rows based on the values in columns

  //creating DF from a raw data
  val df = spark.read.format("json")
    .load(flightPath)

  df.createOrReplaceTempView("dfTable") //view or a virtual table


  //We can also create DataFrames on the fly by taking a set of rows and converting them to a
  //DataFrame
  val myManualSchema = new StructType(Array(
    new StructField("some", StringType, true),
    new StructField("column", StringType, true),
    new StructField("names", LongType, false)))
  val myRows = Seq(Row("Hello", null, 1L),
                  Row("Spark", "some letters", 314L),
                  Row(null, "my data", 13L)
  )
  val myRDD = spark.sparkContext.parallelize(myRows)
  val myDf =  spark.createDataFrame(myRDD, myManualSchema)
  myDf.show()

  //another way of how to convert data from raw data to DF
  //In Scala, we can also take advantage of Spark’s implicits in the console (and if you import them in
  //your JAR code) by running toDF on a Seq type. This does not play well with null types, so it’s not
  //necessarily recommended for production use cases.
  //val anotherDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")




}
