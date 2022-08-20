package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.getOrCreateSpark
import org.apache.spark.ml.feature.{StandardScaler, Tokenizer, VectorAssembler}
import org.apache.spark.sql.functions.{col, concat_ws, explode, expr, lit}
import org.apache.spark.sql.{Column, functions}

object Day33Exercise extends App {

  val spark = getOrCreateSpark("Sparky")

  //Read text from url
  val url = "https://www.gutenberg.org/files/11/11-0.txt"
  val dst = "src/resources/text/Alice.txt"
  Utilities.getTextFromWebAndSave(url, dst)

  //above link shows how to read csv file from url
  //you can adopt it to read a simple text file directly as well
  //alternative download and read from file locally
  //https://www.gutenberg.org/files/11/11-0.txt - Alice in Wonderland
  //https://stackoverflow.com/questions/44961433/process-csv-from-rest-api-into-spark

  //create a DataFrame with a single column called text which contains above book line by line
  val df = spark.read.textFile(dst)
    .toDF("text")

  //create new column called words with will contain Tokenized words of text column
  val tknDF = new Tokenizer()
    .setInputCol("text")
    .setOutputCol("tokenizedWords")

  //create column called textLen which will be a character count in text column
  //https://spark.apache.org/docs/2.3.0/api/sql/index.html#length can use or also length function from spark
  //create column wordCount which will be a count of words in words column //can use count or length - words column will be Array type

  val tknDFWithColumns = tknDF.transform(df.select("text"))
    .withColumn("textLen", expr("CHAR_LENGTH(text)"))
    .withColumn("wordCount", functions.size(col("tokenizedWords")))
//    .show()

  //create Vector Assembler which will transform textLen and wordCount into a column called features
  //features column will have a Vector with two of those values
  val va = new VectorAssembler()
    .setInputCols(Array("textLen", "wordCount"))
    .setOutputCol("features") //otherwise we will get a long hash type column name
  val dfAssembled = va.transform(tknDFWithColumns)
//  dfAssembled.show()

  //create StandardScaler which will take features column and output column called scaledFeatures
  //it should be using mean and variance (so both true)
  val ss = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithStd(true)
    .setWithMean(true)
  val finalDF = ss.fit(dfAssembled).transform(dfAssembled)

  //create a dataframe with all these columns - save to alice.csv file with all columns

//  finalDF.printSchema() //tokenizedWords - Array
  //casting to String, so that the error disappears: Exception: CSV data source does not support map<string,bigint> data type

  val AliceDF = finalDF
    .withColumn("text", col("text").cast("string"))
    .withColumn("textLen", col("textLen").cast("string"))
    .withColumn("wordCount", col("wordCount").cast("string"))
    .withColumn("tokenizedWordsString", col("tokenizedWords").cast("string"))
    .withColumn("features", col("features").cast("string"))
    .withColumn("scaledFeatures", col("scaledFeatures").cast("string"))
    .drop("tokenizedWords")

  AliceDF
    .write
    .format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save("src/resources/text/csv/Alice.csv")

}
