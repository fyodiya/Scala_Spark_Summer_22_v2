package com.github.fyodiya

import org.apache.spark.sql.functions.{col, udf}

object Day27UserDefinedFunctions extends App {

  println("Chapter 6: UDFs - User Defined Functions!")
  val spark = SparkUtilities.getOrCreateSpark("UDFs playground")
  //  spark.conf.set("spark.sql.caseSensitive", true)
  val df = spark.range(10).toDF("num")
  df.printSchema()
  df.show()

  //User-Defined Functions
  //One of the most powerful things that you can do in Spark is define your own functions. These
  //user-defined functions (UDFs) make it possible for you to write your own custom
  //transformations using Python or Scala and even use external libraries. UDFs can take and return
  //one or more columns as input. Spark UDFs are incredibly powerful because you can write them
  //in several different programming languages; you do not need to create them in an esoteric format
  //or domain-specific language. They’re just functions that operate on the data, record by record.
  //By default, these functions are registered as temporary functions to be used in that specific
  //SparkSession or Context.
  //Although you can write UDFs in Scala, Python, or Java, there are performance considerations
  //that you should be aware of. To illustrate this, we’re going to walk through exactly what happens
  //when you create UDF, pass that into Spark, and then execute code using that UDF.
  //The first step is the actual function. We’ll create a simple one for this example. Let’s write a
  //power3 function that takes a number and raises it to a power of three:

  def power3(n: Double): Double = n*n*n //we can make up our own formulas here
  println(power3(10))

  def power3int(n: Long): Long = n*n*n
  println(power3int(10))

//In this trivial example, we can see that our functions work as expected. We are able to provide an
  //individual input and produce the expected result (with this simple test case). Thus far, our
  //expectations for the input are high: it must be a specific type and cannot be a null value (see
  //“Working with Nulls in Data”).
  //Now that we’ve created these functions and tested them, we need to register them with Spark so
  //that we can use them on all of our worker machines. Spark will serialize the function on the
  //driver and transfer it over the network to all executor processes. This happens regardless of
  //language.
  //When you use the function, there are essentially two different things that occur. If the function is
  //written in Scala or Java, you can use it within the Java Virtual Machine (JVM). This means that
  //there will be little performance penalty aside from the fact that you can’t take advantage of code
  //generation capabilities that Spark has for built-in functions. There can be performance issues if
  //you create or use a lot of objects

  //First, we
  //need to register the function to make it available as a DataFrame function:
  val power3udf = udf(power3(_:Double):Double)
  val power3Int_udf = udf(power3int(_:Long):Long)

  //Then, we can use it in our DataFrame code:
  df
    .withColumn("numCubed", power3udf(col("num")))
    .withColumn("numCubedInteger", power3Int_udf(col("num")))
      .show()

  //At this juncture, we can use this only as a DataFrame function. That is to say, we can’t use it
  //within a string expression, only on an expression. However, we can also register this UDF as a
  //Spark SQL function. This is valuable because it makes it simple to use this function within SQL
  //as well as across languages.

  //Let’s register the function in Scala:
  spark.udf.register("power3", power3(_:Double):Double)
  df.selectExpr("power3(num)").show(2)

  //lets register our other function with integers
  spark.udf.register("power3int", power3int(_:Long): Long)

  //creating/registering a new view
  df.createOrReplaceTempView("dfTable")

  spark.sql(
    """
      |SELECT *,
      |power3(num),
      |power3int(num)
      |FROM dfTable
      |""".stripMargin)
    .show() //printing out everything

  //TODO create a UDF which converts Fahrenheit to Celsius
  //run it - create a DF with col(tempF) from -40 to 120
  //register your UDF
  //show both created views, show columns starting with F 90 (included) and ending with F 110 (included)
  //use your Uf to create a temp column with the actual temperature

  //double incoming an double as a return



}
