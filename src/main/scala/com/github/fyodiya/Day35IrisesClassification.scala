package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.getOrCreateSpark
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{RFormula, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.DataFrame

object Day35IrisesClassification extends App {

  val spark = getOrCreateSpark("Sparky")

  val filePath = "src/resources/irises/iris.data"
  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .load(filePath)

  df.printSchema()
  df.describe().show(false)
  df.show(5, truncate = false)

  //now that we have data loaded with default column names _c0 ... _c4
  //we need to create two new columns
  //one would be features (which combines all 4 measurements into a Vector of Doubles)
  //and we need to convert the string labels into numeric labels(most likely 0,1,2) again doubles

  val myRFormula = new RFormula() //RFormula is a quicker way of creating needed column
    .setFormula("flower ~ . ")
  //    .setFormula("_c4 ~ . + _c0 + _c1 + _c2 + _c3")

  val ndf = df.withColumnRenamed("_c4", "flower")
  ndf.show(5, truncate = false)

  val fittedRF = myRFormula.fit(ndf)
  val preparedDF = fittedRF.transform(ndf)
  preparedDF.show(false)
  preparedDF.sample(0.1).show(false)

  val Array(train, test) = preparedDF.randomSplit(Array(0.8, 0.2)) //so 80 percent for training and 20 percent for testing

  import org.apache.spark.ml.classification.DecisionTreeClassifier //this Algorithm is like a game of yes/no questions
  //like the party game "21 questions"
  //we could create more models out of different classifiers
  val decTree = new DecisionTreeClassifier() //there are hyperparameters we could adjust but not for now
    .setLabelCol("label")
    .setFeaturesCol("features")

  val fittedModel = decTree.fit(train)
  //this is the hard work here of creating the model

  val testDF = fittedModel.transform(test)
  //here we get some results

  testDF.show(30,truncate = false)
  //we should have roughly 30 (since 20% of 150 is 30)

  //let's now make a features column by using VectorAssembler
  val va = new VectorAssembler()
    .setInputCols(Array("_c0","_c1","_c2","_c3"))
    .setOutputCol("features") //default name is kind of ugly vecAssembler
  val tdf = va.transform(df)
  tdf.show(5, truncate = false)

  //let's convert our string label _c4 into a numerical value

  val labelIndexer = new StringIndexer().setInputCol("_c4").setOutputCol("label")
  val labelDF = labelIndexer.fit(tdf).transform(tdf)
  labelDF.show(5,truncate = false)

  val fittedModel2 = decTree.fit(labelDF) //create a new model but I used ALL of the data!!!
  //so using test dataframe is sort of useless because we alreday learned from the whole dataset, so chance of overfit is extremely

  val fittedDF = fittedModel2.transform(test)
  fittedDF.show(5, truncate = false)

  //bare minimum is to make a prediction with some classifier model is to have a Vector[Double] column default name being features
  fittedModel2.transform(test.select("features")).show(5, truncate = false)

  def showAccuracy(df: DataFrame): Unit = {
    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(df) //in order for this to work we need label and prediction columns
    println(s"DF size: ${df.count()} Accuracy $accuracy - Test Error = ${1.0 - accuracy}")
  }
  showAccuracy(fittedDF)
  showAccuracy(testDF)

  ////so why is this a bit misleading, this 100% accuracy ?

  //TODO fit Irises data set using split of 75% for training and 25% for testing from preparedDF
  //TODO use any other Classification besides Decision Tree (so LogisticRegression would be fine)
  //https://spark.apache.org/docs/3.2.2/ml-classification-regression.html
  //Check Accuracy on the testing set (you can also show accuracy on training set, but that should be 100% :) )

  println("LOGISTIC REGRESSION")

  val Array(train75, test25) = preparedDF.randomSplit(Array(0.75, 0.25))

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
    .setFamily("multinomial")

  val fittedModel_LR = lr.fit(train75)

  val testDF_LR = fittedModel_LR.transform(test25)
  testDF_LR.show(38)

  val fittedModel2_LR = lr.fit(labelDF) //create a new model

  val fittedDF_LR = fittedModel2_LR.transform(test25)
  fittedDF_LR.show(5, truncate = false)

  fittedModel2_LR.transform(test25.select("features")).show(5, truncate = false)

  showAccuracy(fittedDF_LR)
  showAccuracy(testDF_LR)


  println("Random Forest")
  val rfc = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")

  val fittedModelToo = rfc.fit(train)

  val testDFToo = fittedModelToo.transform(test)

  testDFToo.show(50, truncate = false)

  def showAccuracyToo(df: DataFrame): Unit = {
    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(df) //in order for this to work we need label and prediction columns
    println(s"DF size: ${df.count()} Accuracy $accuracy - Test Error = ${1.0 - accuracy}")
  }

  showAccuracyToo(testDFToo)

  val dt = new DecisionTreeClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setMaxDepth(1)

  val fittedDT = dt.fit(train)
  val testedDT = fittedDT.transform(test)
  showAccuracy(testedDT)
  //depth 1 is not recommended unless you have two classes (binary clasificator)
  //and are 1 question confident - this is enough in that case

  println(fittedDT.toDebugString) //this should show the question it asks

  def decisionTreeTester(classifier:DecisionTreeClassifier, train: DataFrame, test: DataFrame, debug:Boolean=true):Unit = {
    val model = classifier.fit(train)
    if (debug) println(model.toDebugString) //show the questions
    val df = model.transform(test)
    showAccuracy(df)
  }

  println("This is our new pipeline tester:")
  (1 to 5).foreach(n => {
    dt.setMaxDepth(n)
    decisionTreeTester(dt, train,test)})

  //lets see if we can print this decision

  val stages = Array(dt)
  val pipeline = new Pipeline().setStages(stages)
  //now we must define what parameters we will test
  val parameters = new ParamGridBuilder()
    .addGrid(dt.maxDepth, (1 to 5).toArray)
    .addGrid(dt.maxBins, Array(8,16,32,64))
    .build()

  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setProbabilityCol("probability")

  val trainValidation = new TrainValidationSplit()
    .setTrainRatio(0.75)
    .setEstimatorParamMaps(parameters)
    .setEstimator(pipeline)
    .setEvaluator(evaluator)

  val tvFitted = trainValidation.fit(train) //here all loops will run, checking all parameters

  val testAccuracy = evaluator.evaluate(tvFitted.transform(test))
  println(s"Our model is $testAccuracy accurate on test data set.")

  //we have our parameter grid, but which are the best parameters?

  //we are getting the best model
  val trainedPipeline = tvFitted.bestModel.asInstanceOf[PipelineModel]

  //we had only 1 stage in our grid, stage 0
  val ourBestDecisionTree = trainedPipeline.stages(0).asInstanceOf[DecisionTreeClassificationModel]

  println(ourBestDecisionTree.toDebugString)
}
