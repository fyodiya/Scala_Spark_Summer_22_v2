package com.github.fyodiya

import org.apache.spark.sql.SparkSession

object SparkUtilities {

  /**
   * returns a new Spark session
   * @param appName name of our Spark instance
   * @param partitionCount by default is 5
   * @param verbose prints info for debugging purposes
   * @return sparkSession
   */
  def getOrCreateSpark(appName: String, partitionCount: Int = 5, verbose:Boolean = true): SparkSession = {
    if (verbose) println(s"$appName with Scala version: ${util.Properties.versionNumberString}")
    val sparkSession = SparkSession.builder().appName(appName).master("local").getOrCreate()
    sparkSession.conf.set("spark.sql.shuffle.partitions", partitionCount)
    if (verbose) println(s"Session started on Spark version ${util.Properties.versionNumberString} with ${partitionCount} partitions")
    sparkSession
  }



}
