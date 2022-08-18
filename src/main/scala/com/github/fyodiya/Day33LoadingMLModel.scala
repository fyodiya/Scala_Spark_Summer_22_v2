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

  //Deployment Patterns

  //Train your machine learning (ML) model offline and then supply it with offline data. In
  //this context, we mean offline data to be data that is stored for analysis, and not data that
  //you need to get an answer from quickly. Spark is well suited to this sort of deployment.
  //Train your model offline and then put the results into a database (usually a key-value
  //store). This works well for something like recommendation but poorly for something
  //like classification or regression where you cannot just look up a value for a given user
  //but must calculate one based on the input.

  //Train your ML algorithm offline, persist the model to disk, and then use that for serving.
  //This is not a low-latency solution if you use Spark for the serving part, as the overhead
  //of starting up a Spark job can be high, even if you’re not running on a cluster.
  //Additionally this does not parallelize well, so you’ll likely have to put a load balancer in
  //front of multiple model replicas and build out some REST API integration yourself.
  //There are some interesting potential solutions to this problem, but no standards currently
  //exist for this sort of model serving.

  //Manually (or via some other software) convert your distributed model to one that can
  //run much more quickly on a single machine. This works well when there is not too
  //much manipulation of the raw data in Spark but can be hard to maintain over time.
  //Again, there are several solutions in progress. For example, MLlib can export some
  //models to PMML, a common model interchange format.

  //Train your ML algorithm online and use it online. This is possible when used in
  //conjunction with Structured Streaming, but can be complex for some models.

}
