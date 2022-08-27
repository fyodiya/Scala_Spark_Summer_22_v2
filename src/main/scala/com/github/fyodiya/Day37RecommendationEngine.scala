package com.github.fyodiya

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.ml.recommendation.ALS

object Day37RecommendationEngine extends App {

  println("Chapter 28: Recommendations!")
  val spark = SparkUtilities.getOrCreateSpark("sparky")

  val ratings = spark.read.textFile("src/resources/csv/sample_movielens_ratings.txt")
    .selectExpr("split(value , '::') as col")
    .selectExpr(
      "cast(col[0] as int) as userId",
      "cast(col[1] as int) as movieId",
      "cast(col[2] as float) as rating",
      "cast(col[3] as long) as timestamp")
  val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

  training.show(10, truncate = false)
  training.describe().show(truncate = false)

  val als = new ALS()
    .setMaxIter(5)
    .setRegParam(0.01) //default was 0.1 used to avoid overfitting
    .setUserCol("userId")
    .setItemCol("movieId")
    .setRatingCol("rating")
  println(als.explainParams())
  val alsModel = als.fit(training)
  val predictions = alsModel.transform(test)

  //let's see our predictions
  predictions.show(10, truncate = false)

//let's think hot to interpret these predictions

  //we can now output the top K recommendations for each user or movie. The model’s
  //recommendForAllUsers method returns a DataFrame of a userId, an array of
  //recommendations, as well as a rating for each of those movies. recommendForAllItems returns
  //a DataFrame of a movieId, as well as the top users for that movie:

  //5 recommendations for all users
  alsModel.recommendForAllUsers(5)
    .show(10, truncate = false)

  //top 10 users who like each movie (influencers)
  alsModel.recommendForAllItems(10)
    .show(10, truncate = false)

//using explode or slicing an array into rows
  alsModel.recommendForAllUsers(3)
    .selectExpr("userId", "explode(recommendations)")
    .show(10, truncate = false)
  alsModel.recommendForAllItems(3)
    .selectExpr("movieId", "explode(recommendations)")
    .show(10, truncate = false)

  //Evaluators for Recommendation
  //When covering the cold-start strategy, we can set up an automatic model evaluator when
  //working with ALS. One thing that may not be immediately obvious is that this recommendation
  //problem is really just a kind of regression problem. Since we’re predicting values (ratings) for
  //given users, we want to optimize for reducing the total difference between our users’ ratings and
  //the true values. We can do this using the same RegressionEvaluator that we saw in
  //Chapter 27. You can place this in a pipeline to automate the training process. When doing this,
  //you should also set the cold-start strategy to be drop instead of NaN and then switch it back to
  //NaN when it comes time to actually make predictions in your production system:

  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse")
    .setLabelCol("rating")
    .setPredictionCol("prediction")
  val rmse = evaluator.evaluate(predictions)
  println(s"Root-mean-square error = $rmse")
  //we would like to play with hyperparameters and try to minimize this RMSE

  //Frequent Pattern Mining
  //In addition to ALS, another tool that MLlib provides for creating recommendations is frequent
  //pattern mining. Frequent pattern mining, sometimes referred to as market basket analysis, looks
  //at raw data and finds association rules. For instance, given a large number of transactions it
  //might identify that users who buy hot dogs almost always purchase hot dog buns. This technique
  //can be applied in the recommendation context, especially when people are filling shopping carts
  //(either on or offline). Spark implements the FP-growth algorithm for frequent pattern mining.
  //See the Spark documentation and ESL 14.2 for more information about this algorithm.

  import spark.implicits._ //to convert Seq TDF
  val dataset = spark.createDataset(Seq(
    "milk cookies chocolate",
    "milk cookies cocoa chocolate",
    "milk cookies",
    "milk")
  ).map(t => t.split(" ")).toDF("items")

  val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.5).setMinConfidence(0.6)
  val model = fpgrowth.fit(dataset)

  // Display frequent itemsets.
  model.freqItemsets.show()

  // Display generated association rules.
  model.associationRules.show()

  // transform examines the input items against all the association rules and summarize the
  // consequents as prediction
  model.transform(dataset).show()


  //TODO check how large dataset we can mine


}
