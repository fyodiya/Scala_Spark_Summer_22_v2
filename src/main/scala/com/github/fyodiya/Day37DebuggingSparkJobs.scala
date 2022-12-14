package com.github.fyodiya

import scala.io.StdIn.readLine

object Day37DebuggingSparkJobs extends App {

  val spark = SparkUtilities.getOrCreateSpark("sparky")

  //I can change configuration later on
  //https://spark.apache.org/docs/latest/configuration.html#memory-management
  //spark.conf.set("spark.cleaner.periodicGC.interval", "1min") //so spark will be aggressive in cleaning up memory
  //default is every 30mins

  spark.conf.set("spark.sql.shuffle.partitions", 5)

  readLine("Press Enter to quit")

}
