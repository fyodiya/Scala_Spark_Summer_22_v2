package com.github.fyodiya

import org.apache.spark.sql.{SparkSession, functions}

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
  //et level to warning for both file and console

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



}
