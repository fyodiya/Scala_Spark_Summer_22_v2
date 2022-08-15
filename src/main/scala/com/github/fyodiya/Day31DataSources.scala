package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.getOrCreateSpark
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Day31DataSources extends App {

  println("Chapter 8: Reading and writing data sources!")
  val spark = SparkUtilities.getOrCreateSpark("Playground for reading/writing data sources")


  //The goal of this chapter is to give you the ability to read and write from Spark’s core data
  //sources and know enough to understand what you should look for when integrating with thirdparty data sources. To achieve this, we will focus on the core concepts that you need to be able to
  //recognize and understand.

  //The Structure of the Data Sources API
  //Before proceeding with how to read and write from certain formats, let’s visit the overall
  //organizational structure of the data source APIs.
  //Read API Structure
  //The core structure for reading data is as follows:
  //DataFrameReader.format(...).option("key", "value").schema(...).load()

  //We will use this format to read from all of our data sources. format is optional because by
  //default Spark will use the Parquet format. option allows you to set key-value configurations to
  //parameterize how you will read data. Lastly, schema is optional if the data source provides a
  //schema or if you intend to use schema inference. Naturally, there are some required options for
  //each format, which we will discuss when we look at each format.

//Basics of Reading Data
  //The foundation for reading data in Spark is the DataFrameReader. We access this through the
  //SparkSession via the read attribute:
  //spark.read
  //After we have a DataFrame reader, we specify several values:
  //The format
  //The schema
  //The read mode
  //A series of options
  //The format, options, and schema each return a DataFrameReader that can undergo further
  //transformations and are all optional, except for one option. Each data source has a specific set of
  //options that determine how the data is read into Spark (we cover these options shortly). At a
  //minimum, you must supply the DataFrameReader a path to from which to read.
  //Here’s an example of the overall layout:
  //spark.read.format("csv")
  //.option("mode", "FAILFAST")
  //.option("inferSchema", "true")
  //.option("path", "path/to/file(s)")
  //.schema(someSchema)
  //.load()

  //There are a variety of ways in which you can set options; for example, you can build a map and
  //pass in your configurations. For now, we’ll stick to the simple and explicit way that you just saw.

  //Read modes
  //Reading data from an external source naturally entails encountering malformed data, especially
  //when working with only semi-structured data sources. Read modes specify what will happen
  //when Spark does come across malformed records

  //The default read mode is permissive.


  //Write API Structure
  //The core structure for writing data is as follows:
  //DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy(
  //...).save()
  //We will use this format to write to all of our data sources. format is optional because by default,
  //Spark will use the arquet format. option, again, allows us to configure how to write out our
  //given data. PartitionBy, bucketBy, and sortBy work only for file-based data sources; you can
  //use them to control the specific layout of files at the destination.

  //Basics of Writing Data
  //The foundation for writing data is quite similar to that of reading data. Instead of the
  //DataFrameReader, we have the DataFrameWriter. Because we always need to write out some
  //given data source, we access the DataFrameWriter on a per-DataFrame basis via the write
  //attribute:

//  dataFrame.write

  //After we have a DataFrameWriter, we specify three values: the format, a series of options,
  //and the save mode. At a minimum, you must supply a path. We will cover the potential for
  //options, which vary from data source to data source, shortly.

//  dataFrame.write.format("csv")
//    .option("mode", "OVERWRITE")
//    .option("dateFormat", "yyyy-MM-dd")
//    .option("path", "path/to/file(s)")
//    .save()

  //Save modes
  //Save modes specify what will happen if Spark finds data at the specified location (assuming all
  //else equal)

  //The default save mode is errorIfExists. This means that if Spark finds data at the location to which
  //you’re writing, it will fail the write immediately.
  //We’ve largely covered the core concepts that you’re going to need when using data sources, so
  //now let’s dive into each of Spark’s native data sources.

  //CSV Files
  //CSV stands for comma-separated values. This is a common text file format in which each line
  //represents a single record, and commas separate each field within a record. CSV files, while
  //seeming well structured, are actually one of the trickiest file formats you will encounter because
  //not many assumptions can be made in production scenarios about what they contain or how they
  //are structured. For this reason, the CSV reader has a large number of options. These options give
  //you the ability to work around issues like certain characters needing to be escaped—for example,
  //commas inside of columns when the file is also comma-delimited or null values labeled in an
  //unconventional way.

  spark.read.format("csv")

  val myManualSchema = new StructType(Array(
    new StructField("DEST_COUNTRY_NAME", StringType, nullable = true),
    new StructField("ORIGIN_COUNTRY_NAME", StringType, nullable = true),
    new StructField("count", LongType, nullable = false)
  ))

  val csvDF = spark.read.format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST") //so fail on malformed data not matching our schema for one
    .schema(myManualSchema)
    .load("src/resources/flight-data/csv/2010-summary.csv") //we have our DataFrame at this point

  csvDF.show(5)

  csvDF.describe().show()

  //Things get tricky when we don’t expect our data to be in a certain format, but it comes in that
  //way, anyhow.
  //In general, Spark will fail only at job execution time rather than DataFrame definition time—
  //even if, for example, we point to a file that does not exist. This is due to lazy evaluation

  //For instance, we can take our CSV file and write it out as a TSV file quite easily:
  csvDF.write
    .format("csv")
    .mode("overwrite")
    .option("sep", "\t")
    .option("header", value = true) //we want to preserve headers
    .save("src/resources/tmp/my-tsv-file.tsv") //this way Spark will create needed folders if they do not exist

  //JSON Files
  //Those coming from the world of JavaScript are likely familiar with JavaScript Object Notation,
  //or JSON, as it’s commonly called. There are some catches when working with this kind of data
  //that are worth considering before we jump in. In Spark, when we refer to JSON files, we refer to
  //line-delimited JSON files. This contrasts with files that have a large JSON object or array per
  //file.
  //The line-delimited versus multiline trade-off is controlled by a single option: multiLine. When
  //you set this option to true, you can read an entire file as one json object and Spark will go
  //through the work of parsing that into a DataFrame. Line-delimited JSON is actually a much more
  //stable format because it allows you to append to a file with a new record (rather than having to
  //read in an entire file and then write it out), which is what we recommend that you use. Another
  //key reason for the popularity of line-delimited JSON is because JSON objects have structure,
  //and JavaScript (on which JSON is based) has at least basic types. This makes it easier to work
  //with because Spark can make more assumptions on our behalf about the data. You’ll notice that
  //there are significantly less options than we saw for CSV because of the objects

  //https://spark.apache.org/docs/latest/sql-data-sources-json.html

  val jsonDF = spark.read
    .format("json")
    .option("mode", "FAILFAST") //a fail on error instead of making null values
    .schema(myManualSchema) //optional
    .load("src/resources/flight-data/json/2010-summary.json")

  jsonDF.describe().show()
  jsonDF.show(5)

  //Writing JSON Files
  //Writing JSON files is just as simple as reading them, and, as you might expect, the data source
  //does not matter. Therefore, we can reuse the CSV DataFrame that we created earlier to be the
  //source for our JSON file. This, too, follows the rules that we specified before: one file per
  //partition will be written out, and the entire DataFrame will be written out as a folder. It will also
  //have one JSON object per line:

  csvDF.orderBy(desc("count"))
    .limit(10) //just top 10 rows of our current DataFrame ordered descending by count
    .write
    .format("json")
    .mode("overwrite")
    .save("src/resources/tmp/my-json-file.json")

  //no need for headers since the header information is embedded inside the rows

}