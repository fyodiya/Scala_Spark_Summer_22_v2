package com.github.fyodiya

import java.sql.DriverManager

object Day31SQLite extends App {

  println("Chapter 9: SQL databases!")
  val spark = SparkUtilities.getOrCreateSpark("Playground for SQL databases")

  //SQL Databases
  //SQL datasources are one of the more powerful connectors because there are a variety of systems
  //to which you can connect (as long as that system speaks SQL). For instance you can connect to a
  //MySQL database, a PostgreSQL database, or an Oracle database. You also can connect to
  //SQLite, which is what we’ll do in this example. Of course, databases aren’t just a set of raw files,
  //so there are more options to consider regarding how you connect to the database. Namely you’re
  //going to need to begin considering things like authentication and connectivity (you’ll need to
  //determine whether the network of your Spark cluster is connected to the network of your
  //database system).
  //To avoid the distraction of setting up a database for the purposes of this book, we provide a
  //reference sample that runs on SQLite. We can skip a lot of these details by using SQLite,
  //because it can work with minimal setup on your local machine with the limitation of not being
  //able to work in a distributed setting. If you want to work through these examples in a distributed
  //setting, you’ll want to connect to another kind of database.

  //To read and write from these databases, you need to do two things: include the Java Database
  //Connectivity (JDBC) driver for you particular database on the spark classpath, and provide the
  //proper JAR for the driver itself.

  //Reading from SQL Databases
  //When it comes to reading a file, SQL databases are no different from the other data sources that
  //we looked at earlier. As with those sources, we specify the format and options, and then load in
  //the data:
  val driver = "org.sqlite.JDBC"
  val path = "src/resources/flight-data/jdbc/my-sqlite.db"
  val url = s"jdbc:sqlite:${path}" //for other SQL databases you would add username and authentification here
  val tablename = "flight_info"

  //After you have defined the connection properties, you can test your connection to the database
  //itself to ensure that it is functional. This is an excellent troubleshooting technique to confirm that
  //your database is available to (at the very least) the Spark driver. This is much less relevant for
  //SQLite because that is a file on your machine but if you were using something like MySQL, you
  //could test the connection with the following

  //just to test our SQLite connection
//  val connection = DriverManager.getConnection(url)
//  connection.isClosed()
//  println("SQL connection is closed:", connection.isClosed())
//  connection.close()

//  If this connection succeeds, you’re good to go. Let’s go ahead and read the DataFrame from the
//  SQL table:

  val dbDataFrame = spark.read
    .format("jdbc").option("url", url)
    .option("dbtable", tablename)
    .option("driver", driver)
    .load()

  dbDataFrame.printSchema()
  dbDataFrame.describe().show()
  dbDataFrame.show(5, truncate = false)



}
