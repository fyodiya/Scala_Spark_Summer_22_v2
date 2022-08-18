package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.getOrCreateSpark
import org.apache.spark.ml.tuning.TrainValidationSplitModel

object Day33LoadingMLModel extends App {

  println("Chapter 24: Loading ML model.")

  val spark = getOrCreateSpark("sparky")

  //After writing out the model, we can load it into another Spark program to make predictions. To
  //do this, we need to use a “model” version of our particular algorithm to load our persisted model
  //from disk. If we were to use CrossValidator, we’d have to read in the persisted version as the
  //CrossValidatorModel, and if we were to use LogisticRegression manually we would have
  //to use LogisticRegressionModel. In this case, we use TrainValidationSplit, which outputs
  //TrainValidationSplitModel:
  val modelPath = "src/resources/tmp/modelLocation"
  //remember that we don't have this model in git, so we have to save oit first in our previous app
  val model = TrainValidationSplitModel.load(modelPath)

  val df = spark.read.json("src/resources/simple-ml")

  val transformedModel = model.transform(df)
  transformedModel.show(10, truncate = false)


}
