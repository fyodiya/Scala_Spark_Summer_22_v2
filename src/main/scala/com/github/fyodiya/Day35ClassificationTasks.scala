package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.getOrCreateSpark
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, DecisionTreeClassifier, LogisticRegression}
import org.apache.spark.ml.linalg.Vectors

object Day35ClassificationTasks extends App {

  println("Chapter 26: Classification!")
  val spark = getOrCreateSpark("Sparky")

  //Classification is the task of predicting a label, category, class, or discrete variable given some
  //input features. The key difference from other ML tasks, such as regression, is that the output
  //label has a finite set of possible values (e.g., three classes).

  //Types of Classification
  //Before we continue, let’s review several different types of classification.
  //Binary Classification
  //The simplest example of classification is binary classification, where there are only two labels
  //you can predict. One example is fraud analytics, where a given transaction can be classified as
  //fraudulent or not; or email spam, where a given email can be classified as spam or not spam.
  //Multiclass Classification
  //Beyond binary classification lies multiclass classification, where one label is chosen from more
  //than two distinct possible labels. A typical example is Facebook predicting the people in a given
  //photo or a meterologist predicting the weather (rainy, sunny, cloudy, etc.). Note how there is
  //always a finite set of classes to predict; it’s never unbounded. This is also called multinomial
  //classification.
  //Multilabel Classification
  //Finally, there is multilabel classification, where a given input can produce multiple labels. For
  //example, you might want to predict a book’s genre based on the text of the book itself. While
  //this could be multiclass, it’s probably better suited for multilabel because a book may fall into
  //multiple genres. Another example of multilabel classification is identifying the number of objects
  //that appear in an image. Note that in this example, the number of output predictions is not
  //necessarily fixed, and could vary from image to image.

  //Spark has several models available for performing binary and multiclass classification out of the
  //box. The following models are available for classification in Spark:
  //Logistic regression
  //Decision trees
  //Random forests
  //Gradient-boosted trees
  //Spark does not support making multilabel predictions natively. In order to train a multilabel
  //model, you must train one model per label and combine them manually. Once manually
  //constructed, there are built-in tools that support measuring these kinds of models (discussed at
  //the end of the chapter)

  //This chapter will cover the basics of each of these models by providing:
  //A simple explanation of the model and the intuition behind it
  //Model hyperparameters (the different ways we can initialize the model)
  //Training parameters (parameters that affect how the model is trained)
  //Prediction parameters (parameters that affect how predictions are made)

  val bInput = spark.read.format("parquet").load("src/resources/binary-classification")
    .selectExpr("features", "cast(label as double) as label")

  bInput.show(false)

  //Logistic Regression
  //Logistic regression is one of the most popular methods of classification. It is a linear method that
  //combines each of the individual inputs (or features) with specific weights (these weights are
  //generated during the training process) that are then combined to get a probability of belonging to
  //a particular class. These weights are helpful because they are good representations of feature
  //importance; if you have a large weight, you can assume that variations in that feature have a
  //significant effect on the outcome (assuming you performed normalization). A smaller weight
  //means the feature is less likely to be important

  //Training Parameters
  //Training parameters are used to specify how we perform our training. Here are the training
  //parameters for logistic regression.
  //maxIter
  //Total number of iterations over the data before stopping. Changing this parameter probably
  //won’t change your results a ton, so it shouldn’t be the first parameter you look to adjust. The
  //default is 100.
  //tol
  //This value specifies a threshold by which changes in parameters show that we optimized our
  //weights enough, and can stop iterating. It lets the algorithm stop before maxIter iterations.
  //The default value is 1.0E-6. This also shouldn’t be the first parameter you look to tune.
  //weightCol
  //The name of a weight column used to weigh certain rows more than others. This can be a
  //useful tool if you have some other measure of how important a particular training example is
  //and have a weight associated with it. For example, you might have 10,000 examples where
  //you know that some labels are more accurate than others. You can weigh the labels you
  //know are correct more than the ones you don’t.
  //Prediction Parameters
  //These parameters help determine how the model should actually be making predictions at
  //prediction time, but do not affect training. Here are the prediction parameters for logistic
  //regression:
  //threshold
  //A Double in the range of 0 to 1. This parameter is the probability threshold for when a given
  //class should be predicted. You can tune this parameter according to your requirements to
  //balance between false positives and false negatives. For instance, if a mistaken prediction
  //would be costly—you might want to make its prediction threshold very high.
  //thresholds
  //This parameter lets you specify an array of threshold values for each class when using
  //multiclass classification. It works similarly to the single threshold parameter described
  //previously.

  //Example
  //Here’s a simple example using the LogisticRegression model. Notice how we didn’t specify any
  //parameters because we’ll leverage the defaults and our data conforms to the proper column
  //naming. In practice, you probably won’t need to change many of the parameters:

  val lr = new LogisticRegression()
  //.setMaxIter(50) you can set additional parameters later, as well
  println(lr.explainParams()) // see all parameters
  //you can also use lr to set various hyperparameters
  lr.setMaxIter(50)
    .setElasticNetParam(0.7)

  val lrModel = lr.fit(bInput)

  //Once the model is trained you can get information about the model by taking a look at the
  //coefficients and the intercept. The coefficients correspond to the individual feature weights (each
  //feature weight is multiplied by each respective feature to compute the prediction) while the
  //intercept is the value of the italics-intercept (if we chose to fit one when specifying the model).
  //Seeing the coefficients can be helpful for inspecting the model that you built and comparing how
  //features affect the prediction

  println(lrModel.coefficients)
  println(lrModel.intercept)

  val myVector = Vectors.dense(1.0, 5.0, 9.6)
  println(lrModel.predict(myVector)) //will provide an answer of 0 or 1 on this case
  //then you must decide what 0 and 1 represents


  //For a multinomial model (the current one is binary), lrModel.coefficientMatrix and
  //lrModel.interceptVector can be used to get the coefficients and intercept. These will return
  //Matrix and Vector types representing the values or each of the given classes.

  //Model Summary
  //Logistic regression provides a model summary that gives you information about the final, trained
  //model. This is analogous to the same types of summaries we see in many R language machine
  //learning packages. The model summary is currently only available for binary logistic regression
  //problems, but multiclass summaries will likely be added in the future. Using the binary
  //summary, we can get all sorts of information about the model itself including the area under the
  //ROC curve, the f measure by threshold, the precision, the recall, the recall by thresholds, and the
  //ROC curve. Note that for the area under the curve, instance weighting is not taken into account,
  //so if you wanted to see how you performed on the values you weighed more highly, you’d have
  //to do that manually. This will probably change in future Spark versions. You can see the
  //summary using the following APIs:

  val summary = lrModel.summary
  val bSummary = summary.asInstanceOf[BinaryLogisticRegressionSummary]
  println(bSummary.areaUnderROC)
  bSummary.roc.show()
  bSummary.pr.show()

  //Decision Trees
  //Decision trees are one of the more friendly and interpretable models for performing classification
  //because they’re similar to simple decision models that humans use quite often. For example, if
  //you have to predict whether or not someone will eat ice cream when offered, a good feature
  //might be whether or not that individual likes ice cream. In pseudocode, if
  //person.likes(“ice_cream”), they will eat ice cream; otherwise, they won’t eat ice cream. A
  //decision tree creates this type of structure with all the inputs and follows a set of branches when
  //it comes time to make a prediction. This makes it a great starting point model because it’s easy to
  //reason about, easy to inspect, and makes very few assumptions about the structure of the data. In
  //short, rather than trying to train coefficients in order to model a function, it simply creates a big
  //tree of decisions to follow at prediction time. This model also supports multiclass classification
  //and provides outputs as predictions and probabilities in two different columns.
  //While this model is usually a great start, it does come at a cost. It can overfit data extremely
  //quickly. By that we mean that, unrestrained, the decision tree will create a pathway from the start
  //based on every single training example. That means it encodes all of the information in the
  //training set in the model. This is bad because then the model won’t generalize to new data (you
  //will see poor test set prediction performance). However, there are a number of ways to try and
  //rein in the model by limiting its branching structure (e.g., limiting its height) to get good
  //predictive power

  //Model parameters
  //impurity, maxBins, maxDepth, minInfoGain etc.

  //Training Parameters
  //These are configurations we specify in order to manipulate how we perform our training. Here is
  //the training parameter for decision trees:
  //checkpointInterval
  //Checkpointing is a way to save the model’s work over the course of training so that if nodes
  //in the cluster crash for some reason, you don’t lose your work. A value of 10 means the
  //model will get checkpointed every 10 iterations. Set this to -1 to turn off checkpointing. This
  //parameter needs to be set together with a checkpointDir (a directory to checkpoint to) and
  //with useNodeIdCache=true. Consult the Spark documentation for more information on
  //checkpointing.

  //Prediction Parameters
  //There is only one prediction parameter for decision trees: thresholds.

  val dt = new DecisionTreeClassifier()
    .setMaxDepth(5) //default is 5, so the tree is not too large
  println(dt.explainParams())

  val dtModel = dt.fit(bInput)
  //decision tree predictor will work just like the logistic regression model
  println(dtModel.predict(myVector))

}
