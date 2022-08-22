package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.getOrCreateSpark
import org.apache.spark.ml.feature.{CountVectorizer, StopWordsRemover, Tokenizer}

object Day34Exercise extends App {

  val spark = getOrCreateSpark("Sparky")

  //use tokenized alice - from weekend exercise
  val url = "https://www.gutenberg.org/files/11/11-0.txt"
  val dst = "src/resources/text/Alice.txt"
  Utilities.getTextFromWebAndSave(url, dst)
  val df = spark.read.textFile(dst)
    .toDF("text")
//  df.cache()
  val tkn = new Tokenizer()
    .setInputCol("text")
    .setOutputCol("tokenizedWords")
  val tokenDF = tkn.transform(df.select("text"))

  //remove English stop words
  val englishStopWords = StopWordsRemover.loadDefaultStopWords("english")
  val stops = new StopWordsRemover()
    .setStopWords(englishStopWords)
    .setInputCol("tokenizedWords")
    .setOutputCol("withoutEnglishStopWords")
  stops.transform(tokenDF)

  //Create a CountVectorized of words/tokens/terms that occur in at least 3 documents
  //(here that means rows)
  val cv = new CountVectorizer()
    .setInputCol("tokenizedWords")
    .setOutputCol("countVec")
    .setVocabSize(500)
    .setMinTF(1) //term appears at least once
    .setMinDF(3) //term appears in at least three documents
  val fittedCV = cv.fit(tokenDF)

  //show first 30 rows of data
  fittedCV.transform(tokenDF).show(30, false)
  println(fittedCV.vocabulary.mkString(","))

  //the terms have to occur at least 1 time in each row

}
