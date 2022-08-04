package com.github.fyodiya

import org.apache.spark.sql.functions.{col, from_json, get_json_object, json_tuple, to_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.json4s.scalap.scalasig.ClassFileParser.header

object Day26JSONDataTypes extends App {

  println("Chapter 6: Working with JSON!")
  val spark = SparkUtilities.getOrCreateSpark("JSON sandbox")
  //  spark.conf.set("spark.sql.caseSensitive", true)
  val filePath = "src/resources/retail-data/by-day/2011-12-01.csv"
  val df = SparkUtilities.readDataWithView(spark, filePath)

  //Spark has some unique support for working with JSON data. You can operate directly on strings
  //of JSON in Spark and parse from JSON or extract JSON objects. Let’s begin by creating a JSON
  //column:

  val jsonDF = spark.range(1)
    .selectExpr("""
'{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")

  jsonDF.printSchema()
  jsonDF.show(5, truncate = false)

  jsonDF.select(
    //3 levels deep to extract these 2
    //we got key myJSONkey which brings us another object
    //then we get the value of that
    //finally we have an array access of 2nd value
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
    json_tuple(col("jsonString"), "myJSONKey") as "jtuplecol")
    .show(7, truncate = false)

  //we can go the other way
  //You can also turn a StructType into a JSON string by using the to_json function:

  df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")))
    .show(5, truncate = false)

  //again: JSON basically is just a specially formatted string

  //This function also accepts a dictionary (map) of parameters that are the same as the JSON data
  //source. You can use the from_json function to parse this (or other JSON data) back in. This
  //naturally requires you to specify a schema, and optionally you can specify a map of options, as
  //well:

  //we hand code the schema
  val parseSchema = new StructType(Array(
    StructField("InvoiceNo", StringType, nullable = true),
    StructField("Description", StringType, nullable = true)))

  df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")).alias("newJSON"))
    .select(from_json(col("newJSON"), parseSchema), col("newJSON"))
    .show(5, truncate = false)

  val dfParsedJson =   df.selectExpr("*", "(InvoiceNo, Description) as myStruct")
    .withColumn("newJSON", to_json(col("myStruct")))
    .withColumn("fromJSON", from_json(col("newJSON"), parseSchema ))

  dfParsedJson.printSchema()
  dfParsedJson.show(5, truncate = false)

}
