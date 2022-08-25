package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.getOrCreateSpark
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{DecisionTreeRegressor, LinearRegression}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

object Day35Regression extends App {

  println("Chapter 26: Regressions!")

  val spark = getOrCreateSpark("Sparky")
  val src = "src/resources/csv/range100"

  //Chapter 27. Regression
  //Regression is a logical extension of classification. Rather than just predicting a single value from
  //a set of values, regression is the act of predicting a real number (or continuous variable) from a
  //set of features (represented as numbers).
  //Regression can be harder than classification because, from a mathematical perspective, there are
  //an infinite number of possible output values. Furthermore, we aim to optimize some metric of
  //error between the predicted and true value, as opposed to an accuracy rate. Aside from that,
  //regression and classification are fairly similar. For this reason, we will see a lot of the same
  //underlying concepts applied to regression as we did with classification.
  //Use Cases
  //The following is a small set of regression use cases that can get you thinking about potential
  //regression problems in your own domain:
  //Predicting movie viewership
  //Given information about a movie and the movie-going public, such as how many people
  //have watched the trailer or shared it on social media, you might want to predict how many
  //people are likely to watch the movie when it comes out.
  //Predicting company revenue
  //Given a current growth trajectory, the market, and seasonality, you might want to predict
  //how much revenue a company will gain in the future.
  //Predicting crop yield
  //Given information about the particular area in which a crop is grown, as well as the current
  //weather throughout the year, you might want to predict the total crop yield for a particular
  //plot of land.
  //Regression Models in MLlib
  //There are several fundamental regression models in MLlib. Some of these models are carryovers
  //from Chapter 26. Others are only relevant to the regression problem domain. This list is current
  //as of Spark 2.2 but will grow:
  //Linear regression
  //Generalized linear regression
  //Isotonic regression
  //Decision trees
  //Random forest
  //Gradient-boosted trees
  //Survival regression

  val df = spark.read
    .format("csv")
    .option("header", value = true)
    .option("inferSchema", value = true)
    .load(src) //we could also use option("path", src).load

  df.show(10)

  //now we need to once again prepare features as a vector in this case vector with single value in each row

  val rFormula = new RFormula()
    .setFormula("y ~ .") //so y is the label and rest (here just x col) are the features
    .setLabelCol("value") //default is  label
    .setFeaturesCol("features") //again features is the default already

  val ndf = rFormula
    .fit(df) //prepare the data
    .transform(df) //transform df into a new dataframe

  ndf.show(10)

  val linReg = new LinearRegression()
    .setLabelCol("value") //we could also use y

  println(linReg.explainParams()) //we can check what parameters we can adjust
  //we could also look up here
  //https://spark.apache.org/docs/latest/ml-classification-regression.html#linear-regression


  val lrModel = linReg.fit(ndf) //so this already creates a model we can use for predictions

  //we can already use it to make predictions, just need to pass in Vectors type
  println(lrModel.predict(Vectors.dense(1000)))
  println(lrModel.predict(Vectors.dense(-1000)))

  val summary = lrModel.summary
  summary.residuals.show(10) //residuals are the errors, differences from actual values

  //in a linear regression we want the intercept ax+b - intercept will be b
  //coefficient(s) are the a values - so for multiple features you would have a1, a2, a3, etc
  //like 3 features would be a1x1 + a2x2 + a3x3 + b

  val intercept = lrModel.intercept //this is our b
  val coefficient = lrModel.coefficients(0) //we only have 1 features so first one (a)

  println(s"Intercept is $intercept and coefficient is $coefficient")
  //so we will find out our y = ax + b

  //of course we would also want to have a test set to check our model
  //so we use transform of our model to make prediction on some dataframe (of course we need a features column)
  val predictDF = lrModel.transform(ndf)
    .withColumn("residuals", expr("value - prediction"))

  predictDF.show(10, truncate = false)

  //open "src/resources/csv/range3d"
  val myDF = spark.read
    .format("csv")
    .option("header", value = true)
    .option("inferSchema", value = true)
    .load("src/resources/csv/range3d")

  //Transform x1,x2,x3 into features(you cna use VectorAssembler or RFormula), y can stay, or you can use label/value column

  val myRFormula = new RFormula()
    .setFormula("y ~ .")
    .setLabelCol("value")
    .setFeaturesCol("features")

  val myNewDF = rFormula
    .fit(myDF)
    .transform(myDF)

  myNewDF.show(10)

  //create  a Linear Regression model, fit it to our 3d data

  val myLinReg = new LinearRegression()
    .setLabelCol("value")

  val myLRModel = myLinReg.fit(myNewDF)

  val mySummary = myLRModel.summary
  mySummary.residuals.show(10)

  //print out intercept
  val myIntercept = myLRModel.intercept //this is our b
  val myCoefficient = myLRModel.coefficients(0) //we only have 1 features so first one (a)

  val mySummary2 = myLRModel.summary
  mySummary2.residuals.show(10)
  val intercept2 = myLRModel.intercept
  val coefficient2 = myLRModel.coefficients
  val x1 = coefficient2(0)
  val x2 = coefficient2(1)
  val x3 = coefficient2(2)

  //print out all 3 coefficients
  println(s"My intercept is $intercept and coefficient is $coefficient; " +
    s"Another intercept is $intercept2 and coefficients are $x1, $x2, $x3")

  val myPredictDF = myLRModel.transform(myNewDF)
    .withColumn("residuals", expr("value - prediction"))
  myPredictDF.show(10, truncate = false)


  //make a prediction if values of x1, x2 and x3 are respectively 100, 50, 1000
  println(myLRModel.predict(Vectors.dense(100, 50, 1000)))
  println(myLRModel.predict(Vectors.sparse(3, indices = Array(0,2), values = Array(10.0, 30.0))))
  println(myLRModel.predict(Vectors.dense(10.0, 0.0, 30.0)))

  def testLinearRegression(lr:LinearRegression, df:DataFrame):Unit = {
    val model = lr.fit(df)
    val intercept = model.intercept
    val coefficient = model.coefficients
    val x1 = coefficient(0)
    val x2 = coefficient(1)
    val x3 = coefficient(2)

    //print out all 3 coefficients
    println(s"Intercept is $intercept and coefficients are $x1, $x2, $x3")

    val summary = model.summary
    println("meanAbsoluteErr", summary.meanAbsoluteError, "MSE", summary.meanSquaredError)
    import spark.implicits._ //to make going from regular Seq toDF method available
    //    println(summary.objectiveHistory.toSeq.toDF.show())
    println(summary.objectiveHistory.mkString(","))
  }

  //so let's test our regParameter
  for (v <- Array(0.0, 0.2,0.5,0.7, 1.0)) {
    val lr = new LinearRegression()
      .setRegParam(v)
      .setMaxIter(10)
      //      .setLoss() //you can change this squared error to something else
      .setLabelCol("y") //value would also work
    testLinearRegression(lr, myNewDF)
  }


  val Array(train,test) = ndf.randomSplit(Array(0.75, 0.25))

  //turns out there are Decision Tree Regressors not only Classifiers

  val dt = new DecisionTreeRegressor()
    .setLabelCol("y")
    .setFeaturesCol("features") //we can change our hyperparameters

  val dtModel = dt.fit(train)

  val dtPrediction = dtModel.transform(test)

  println(dtModel.toDebugString)

  dtPrediction.show(20, truncate = false)

  //so decision tree regression will not perform well outside the range of features used for training
  println(dtModel.predict(Vectors.dense(2000))) //so 399 no matter how large
  println(dtModel.predict(Vectors.dense(-2000))) //so 13.148 no matter how small

  //to explore on your own would be Isotonic Regression
  //performs well when x and y values and going up and to the right

  //Isotonic Regression
  //Isotonic regression is another specialized regression model, with some unique requirements.
  //Essentially, isotonic regression specifies a piecewise linear function that is always monotonically
  //increasing. It cannot decrease. This means that if your data is going up and to the right in a given
  //plot, this is an appropriate model. If it varies over the course of input values, then this is not
  //appropriate.
  //The illustration of isotonic regression’s behavior in Figure 27-1 makes it much easier to
  //understand

  //another interesting regressor would be

  //https://spark.apache.org/docs/latest/ml-classification-regression.html#factorization-machines-regressor

}
