package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.{getOrCreateSpark, readDataWithView}
import org.apache.spark.sql.functions.{expr, monotonically_increasing_id}

object Day37CumulativeSum extends App {
  val spark = getOrCreateSpark("Sparky")

  val defaultSrc = "src/resources/csv/fruits.csv"
  //so our src will either be default file  or the first argument supplied by user
  val src = if (args.length >= 1) args(0) else defaultSrc

  println(s"My Src file will be $src")

  val df = readDataWithView(spark, src)
    .withColumn("id",monotonically_increasing_id)
    .withColumn("total", expr("quantity * price"))

  //I have to create the view again since original view does not have total column
  df.createOrReplaceTempView("dfTable")
  df.show()

  val sumDF = spark.sql(
    """
      |SELECT *, SUM(total) OVER
      |(ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as CSUM,
      |ROUND(SUM(total), 2) OVER
      |(PARTITION BY fruit
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as SUMFRUIT
      |FROM dfTable
      |ORDER BY id ASC
      |""".stripMargin)
  //so our sum is over default ordering an no partitions
  sumDF.show(false)


  //do the same using spark function, also check ROUND function

  val sumRounder = sumDF
    .withColumn("SUMFRUIT", expr("ROUND('SUMFRUIT', 2)"))
  //backticks for column names
  //we are overwriting the old column SUMFRUIT

  sumRounder.show()

}