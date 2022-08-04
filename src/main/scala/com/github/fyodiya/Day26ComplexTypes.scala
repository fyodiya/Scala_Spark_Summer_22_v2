package com.github.fyodiya

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{array_contains, col, explode, size, split, struct}

object Day26ComplexTypes extends App {

  //Working with Complex Types
  //Complex types can help you organize and structure your data in ways that make more sense for
  //the problem that you are hoping to solve. There are three kinds of complex types: structs, arrays,
  //and maps.

  println("Chapter 6: Working with complex types!")
  val spark = SparkUtilities.getOrCreateSpark("Day26_Playground")
  //  spark.conf.set("spark.sql.caseSensitive", true)
  val filePath = "src/resources/retail-data/by-day/2011-12-01.csv"
  val df = SparkUtilities.readDataWithView(spark, filePath)

 //Structs
  //You can think of structs as DataFrames within DataFrames. A worked example will illustrate
  //this more clearly. We can create a struct by wrapping a set of columns in parenthesis in a query:
  df.selectExpr("(Description, InvoiceNo) as complex", "*")
  df.selectExpr("struct(Description, InvoiceNo) as complex", "*")

  val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
  complexDF.createOrReplaceTempView("complexDF")
  complexDF.show(10, truncate = false)

  //We now have a DataFrame with a column complex. We can query it just as we might another
  //DataFrame, the only difference is that we use a dot syntax to do so, or the column method
  //getField:
  complexDF.select("complex.Description")
  complexDF.select(col("complex").getField("Description")).show(10, truncate = false)

  //We can also query all values in the struct by using *. This brings up all the columns to the toplevel DataFrame:
  complexDF.select("complex.*")

  //same in SQL
  spark.sql(
    """
      |SELECT complex.* FROM complexDF
      |"""
      .stripMargin)
    .show(10, truncate = false)

//all three different approaches get you the same result

  //Arrays
  //To define arrays, let’s work through a use case. With our current data, our objective is to take
  //every single word in our Description column and convert that into a row in our DataFrame.
  //The first task is to turn our Description column into a complex type, an array.

  //split
  //We do this by using the split function and specify the delimiter:

  //basic engineering task:
  df.select(split(col("Description"), " "))
    .show(5, truncate = false)

  //in SQL
  spark.sql(
    """
      |SELECT split(Description, ' ') as MySplit FROM dfTable
      |""".stripMargin
  ).show(5, truncate = false)

  //This is quite powerful because Spark allows us to manipulate this complex type as another
  //column. We can also query the values of the array using Python-like syntax:
  df.select(split(col("Description"), " ").alias("array_col"))
    .selectExpr("array_col[0] as first", "array_col[1] as second").show(3, truncate = false)

  //in SQL
  spark.sql(
    """
      |SELECT split(Description, ' ')[0] FROM dfTable
      |""".stripMargin)
    .show(5, truncate = false)

  //Array Length
  //We can determine the array’s length by querying for its size:
  df.select(split(col("Description"), " ").alias("array_col"))
    .withColumn("arr_len", size(col("array_col")))
    .show(7, truncate = false) // shows 5 and 3
  //we create a new column on top of an existing column

  //array_contains
  //We can also see whether this array contains a value:
  df.select(split(col("Description"), " ").alias("array_col"))
    .withColumn("arr_len", size(col("array_col"))) //so we add a new column using our only column from previous selction
    .withColumn("white_exists", array_contains(col("array_col"), "WHITE"))
    .show(5, truncate = false)

  //explode
  //The explode function takes a column that consists of arrays and creates one row (with the rest of
  //the values duplicated) per value in the array.
  df.withColumn("splitted", split(col("Description"), " "))
    .withColumn("exploded", explode(col("splitted")))
    .select("Description", "InvoiceNo", "exploded").show(2, truncate = false)
  //new row for each element

  //in SQL
  spark.sql(
    """
      |SELECT Description, InvoiceNo, exploded
      |FROM (SELECT *, split(Description, " ") as splitted FROM dfTable)
      |LATERAL VIEW explode(splitted) as exploded
      |""".stripMargin)
    .show(5, truncate = false)

  //Maps
  //Maps are created by using the map function and key-value pairs of columns. You then can select
  //them just like you might select from an array
  df.select(functions.map(col("Description"), col("InvoiceNo")).alias("complex_map"))
    .show(10, truncate = false)

  //in SQL
  spark.sql(
    """
      |SELECT map(Description, InvoiceNo) as complex_map FROM dfTable
      |WHERE Description IS NOT NULL
      |""".stripMargin)
    .show(10, truncate = false)

  //You can query them by using the proper key. A missing key returns null:
  df.select(functions.map(col("Description"), col("InvoiceNo")).alias("complex_map"))
    .selectExpr("complex_map['WHITE METAL LANTERN']").show(5, truncate = false)

  //You can also explode map types, which will turn them into columns:

  df.select(functions.map(col("Description"), col("InvoiceNo")).alias("complex_map"))
    .selectExpr("explode(complex_map)").show(12, truncate = false)

}
