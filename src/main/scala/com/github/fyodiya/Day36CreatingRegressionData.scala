package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.getOrCreateSpark
import org.apache.spark.sql.functions.expr

object Day36CreatingRegressionData extends App {

  println("Chapter 27: Creating regression data!")
  val spark = getOrCreateSpark("Sparky")

  val dst = "src/resources/csv/range100"

  //no seed is set so each time should be different random noise

  val df = spark
    .range(100)
    .toDF()
    .withColumnRenamed("id", "x")
    .withColumn("y", expr("round(x*4+5+rand()-0.5, 3)")) //so our linear formula has some noise
  //so our formula ix f(x) = 4x+5+some random noise from -0.5 to 0.5

  df.show(10, truncate = false)

  df.write
    .format("csv")
    .option("path", dst)
    .option("header", value = true)
    .mode("overwrite")
    .save

  val df3d = spark.range(100)
    .toDF()
    .withColumnRenamed("id", "x1")
    .withColumn("x2", expr("x1 + 30"))
    .withColumn("x3", expr("x1 - 20"))
    .withColumn("y", expr("round(50 + x1 * 2 + x2 * 3 + x3 * 5 + rand()*2-1, 4)"))

  df3d.show(5, truncate = false)

  df3d.write
    .format("csv")
    .option("path", "src/resources/csv/range3d")
    .option("header", value = true)
    .mode("overwrite")
    .save

  //TODO conditional expressions in spark SQL
  //val dfNonLin = spark
  //    .range(100)
  //    .toDF()
  //    .withColumnRenamed("id", "x")
  //    .withColumn("y", expr("WHEN x < 50 THEN 20 ELSE 70")) //so our linear formula has some noise
  // so our formula ix f(x) = 4x+5+some random noise from -0.5 to 0.5
  //
  //  dfNonLin.show(10, false)



}
