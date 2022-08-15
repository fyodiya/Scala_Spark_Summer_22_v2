package com.github.fyodiya

object Day31Parquet extends App {

  println("Chapter 8: Reading and writing data sources!")
  val spark = SparkUtilities.getOrCreateSpark("Playground for reading/writing Parquet data sources")

  //Parquet is an open source column-oriented data store that provides a variety of storage
  //optimizations, especially for analytics workloads. It provides columnar compression, which
  //saves storage space and allows for reading individual columns instead of entire files. It is a file
  //format that works exceptionally well with Apache Spark and is in fact the default file format. We
  //recommend writing data out to Parquet for long-term storage because reading from a Parquet file
  //will always be more efficient than JSON or CSV. Another advantage of Parquet is that it
  //supports complex types. This means that if your column is an array (which would fail with a
  //CSV file, for example), map, or struct, you’ll still be able to read and write that file without
  //issue.

  //basically, parquet is a zipped CSV file with strong schema and compressed by columns, fast to load and supported by many programs
  //downside - it's binary (non-readable by humans)

  // Here’s how to specify Parquet as the read format:
  spark.read.format("parquet") //we can skip it, since it's default

//Reading Parquet Files
  //Parquet has very few options because it enforces its own schema when storing data. Thus, all you
  //need to set is the format and you are good to go. We can set the schema if we have strict
  //requirements for what our DataFrame should look like. Oftentimes this is not necessary because
  //we can use schema on read, which is similar to the inferSchema with CSV files. However, with
  //Parquet files, this method is more powerful because the schema is built into the file itself (so no
  //inference needed).

  //Here are some simple examples of reading parquet:
  spark.read.format("parquet")
  //or
  val df = spark.read.format("parquet")
        .load("src/resources/flight-data/parquet/2010-summary.parquet")
    //version mismatch generates warnings - creator metadata not preserved
    //https://stackoverflow.com/questions/42320157/warnings-trying-to-read-spark-1-6-x-parquet-into-spark-2-x
//    .load("src/resources/flight-data/parquet/2010-summary_fixed.parquet")

  df.show(5)
  df.describe().show()
  df.printSchema()

  //we will save the result by using our current parquet standard,
  //because we read from the old one and got some warnings
  df.write
    .format("parquet")
    .mode("overwrite") //we also could use .option("mode", "overwrite") but then it's harder to debug typos
    .save("src/resources/flight-data/parquet/2010-summary_fixed.parquet")

  //read parquet file from src/resources/regression
  val homeworkDF = spark.read.format("parquet")
    .load("src/resources/regression")

  //print schema
  homeworkDF.printSchema()

//  val dfSchema = homeworkDF.schema()
//if we need to use schema somewhere else
  homeworkDF.printSchema()

  //print a sample of some rows
  homeworkDF.show(5)

  //show some basic statistics - describe would be a good start
  homeworkDF.describe().show()

  //if you encounter warning reading data THEN save into src/resources/regression_fixed
//csvFile.write.format("parquet").mode("overwrite")
  //.save("src/resources/regression_fixed.parquet")


  //ORC Files
  //ORC is a self-describing, type-aware columnar file format designed for Hadoop workloads. It is
  //optimized for large streaming reads, but with integrated support for finding required rows
  //quickly. ORC actually has no options for reading in data because Spark understands the file
  //format quite well. An often-asked question is: What is the difference between ORC and Parquet?
  //For the most part, they’re quite similar; the fundamental difference is that Parquet is further
  //optimized for use with Spark, whereas ORC is further optimized for Hive.
  spark.read
    .format("orc")
    .load("src/resources/flight-data/orc/2010-summary.orc")
    .show(5)

  //writing ORC files
  df.write.format("orc").mode("overwrite").save("/tmp/my-json-file.orc")

}
