package com.github.fyodiya

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.types.{LongType, IntegerType, DoubleType, BooleanType, StringType, StructField, StructType}

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

//let’s take a look at their most useful methods that
  //you’re going to be using: the select method when you’re working with columns or expressions,
  //and the selectExpr method when you’re working with expressions in strings. Naturally some
  //transformations are not specified as methods on columns; therefore, there exists a group of
  //functions found in the org.apache.spark.sql.functions package.
  //With these three tools, you should be able to solve the vast majority of transformation challenges
  //that you might encounter in DataFrames.

  //select and selectExpr allow you to do the DataFrame equivalent of SQL queries on a table of data

  df.select("DEST_COUNTRY_NAME").show(2)

  val newDF = df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME")
  newDF.show(3)

  //You can select multiple columns by using the same style of query, just add more column name
  //strings to your select method call

  println("Same thing using SQL syntax")
  //with SQL syntax it is a bit harder to debug errors / typos etc
  val sqlWay = spark.sql("""
      SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME as my_ORIGIN
      FROM dfTable
      LIMIT 10
      """)
  sqlWay.show(5)

  //you can refer to columns in a number of different
  //ways; all you need to keep in mind is that you can use them interchangeably
  //try to stick with just one type in a single project
  df.select(
    df.col("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"),
    //    'DEST_COUNTRY_NAME, //not used as much required implicits which are being depreceated
    //    $"DEST_COUNTRY_NAME",
    expr("DEST_COUNTRY_NAME")) //expression lets us do more transformations than col or column
    .show(2)

  //One common error is attempting to mix Column objects and strings. For example, the following
  //code will result in a compiler error:
  //df.select(col("DEST_COUNTRY_NAME"), "DEST_COUNTRY_NAME")

  //expr is the most flexible reference that we can use. It can refer to a plain
  //column or a string manipulation of a column. To illustrate, let’s change the column name, and
  //then change it back by using the AS keyword and then the alias method on the column:

  // in Scala
  df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

  //we can use aliases instead of changing the column name
  // in Scala
  df.select(expr("DEST_COUNTRY_NAME as destination").alias("my destination"))
    .show(2)


  val foodSchema = new StructType(Array(
    new StructField("food", StringType, false), //so true refers to this field/column being nullable namely could have null
    new StructField("qty", IntegerType, false),
    new StructField("price", DoubleType, false),
    new StructField("isVegan", BooleanType, true)))
  //so names have to be present it is not nullable - ie required

  //create a dataFrame called foodFrame which will hold those Rows

  val foodRows = Seq(
    Row("Cottage cheese", 1, 3.49, false), //we need to specify 1L because 1 by itself is an integer
    Row("Sourdough bread", 2, 4.99, true),
    Row("Dumplings", 12, 3.89, null),
    Row("Smoked salmon", 1, 5.69, false)
  )

  val foodRDD = spark.sparkContext.parallelize(foodRows) //you could add multiple partitions(numSlices) here which is silly when you only have 4 rows o data
  val foodFrame = spark.createDataFrame(foodRDD, foodSchema)

  //Use Select or/an SQL syntax to select and show only name and qty

  foodFrame.select(col("food"),
                  col("qty")).show()

  //so 3 Rows of food each Row has 4 entries (columns) so original data could be something like Chocolate, 3, 2.49, false
  //in Schema, name, qty, price are required (not nullable) while isVegan could be null

}