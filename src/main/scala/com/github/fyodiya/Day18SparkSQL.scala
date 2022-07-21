package com.github.fyodiya

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{desc, max}
//import org.apache.spark.sql.functions. //lot of functions you probably do not want to import all of them
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Encoder, Encoders}

// in Scala
case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String,
                  count: BigInt)

object Day18SparkSQL extends App {

  println(s"Reading CSVs with Scala version ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  //spark.sparkContext.setLogLevel("WARN") //if you did not have log4j.xml - set it up in src/main/resources
  //problem with the above-mentioned approach is that it would still spew all the initial configuration debug info
  println(s"Session started on Spark version ${spark.version}")

  val flightData2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/resources/flight-data/csv/2015-summary.csv")

  //  println(flightData2015.take(5).mkString(",")
  println(s"We have ${flightData2015.count()} rows of data!")

  //creating a temporary view of our data
  //by using SQL syntax on our dataframe
  flightData2015.createTempView("flight_data_2015")

  //now we can use SQL syntax in Scala
  val sqlWay = spark.sql("""
      SELECT DEST_COUNTRY_NAME, count(1)
      FROM flight_data_2015
      GROUP BY DEST_COUNTRY_NAME
      """)

  //this is the other approach
  val dataFrameWay = flightData2015
    .groupBy("DEST_COUNTRY_NAME").count()

  sqlWay.show(10)
  dataFrameWay.show(10)

  //set up logging  log4j2.xml config file
  //set level to "warning" for both the file and console

  //open up flight Data from 2014
  val flightData2014 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/resources/flight-data/csv/2014-summary.csv")

  //create SQL view
  flightData2014.createTempView("flight_data_2014")

  //order by flight counts
  val dataSQL2014 = spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count) as FLIGHTS
    FROM flight_data_2014
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY sum(count) DESC
    """)

  //show top 10 flights
  dataSQL2014.show(10)

  //two different approaches to achieve the same result:
  spark.sql("SELECT max(count) from flight_data_2015").show()
  flightData2015.select(functions.max("count")).show()

  val maxSQL = spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY sum(count) DESC
    """)
  maxSQL.show()

  //another approach is this:
//  //FIXME
//  flightData2015
//    .groupBy("DEST_COUNTRY_NAME")
//    .sum("count")
//    .withColumnRenamed("sum(count)", "destination_total")
//    .sort(desc("destination_total"))
//    .toDF()
//    .write
//    .format("CSV")
//    .mode("override")
////    .option("separator", "\t")
//    .save("src/resources/flight-data/csv/top_destinations_2015.csv")
//we can save the results into a file instead of printing them out


//  val flightsDF = spark.read
//  //    .parquet("src/resources/flight-data/parquet/2010-summary.parquet/")
//
//  //one of those dark corners of Scala called implicits, which is a bit magical
//  implicit val enc: Encoder[Flight] = Encoders.product[Flight]
//  //we needed the above line so the below type conversion works
//  val flights = flightsDF.as[Flight]
//  val flightsArray = flights.collect() //now we have local storage of our Flights
//  //now we can use regular Scala methods
//  println(s"We have information on ${flightsArray.length} flights")
//  val sortedFlights = flightsArray.sortBy(_.count)
//  println(sortedFlights.take(5).mkString("\n"))


}