package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.{getOrCreateSpark, readDataWithView}
import org.apache.spark.ml.feature.RFormula

  object Day32Exercise extends App {

    val spark = getOrCreateSpark("Sandbox")

    //load into dataframe from retail-data by-day December 1st
    val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"
    val df = readDataWithView(spark, filePath)

    //create RFormula to use Country as label and only UnitPrice and Quantity as Features
    //make sure they are numeric columns - we do not want one hot encoding here
    //you can leave column names at default

     val formula = new RFormula()
      .setFormula("Country ~ UnitPrice + Quantity")
      .setFeaturesCol("MyFeatures")
      .setLabelCol("MyLabel")

    //create output dataframe with the the formula performing fit and transform
    val outputDF = formula.fit(df).transform(df)
    outputDF.show(false)
    outputDF.printSchema()

    //BONUS
    //try creating features from ALL columns in the Dec1st CSV except of course Country (using . syntax)
    //This should generate very sparse column of features because of one hot encoding

    val anotherFormula = new RFormula()
      .setFormula("Country ~ .")
      .setFeaturesCol("MyFeatures")
      .setLabelCol("MyLabel")

    val outputDFAgain = anotherFormula.fit(df).transform(df)
    outputDFAgain.show()

}
