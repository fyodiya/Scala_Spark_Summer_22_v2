package com.github.fyodiya

import com.github.fyodiya.Day37Clustering.testKMeans
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.RFormula

object Day37ClusteringExercise extends App {


  //TODO find optimal number of segments in the src/resources/csv/cluster_me.csv file
  //Use Silhouette calculations

  val spark = SparkUtilities.getOrCreateSpark("sparky")
  val filePath = "src/resources/csv/cluster_me.csv"

  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .load(filePath)

  val myRFormula = new RFormula().setFormula("irisType ~ . ")
  val fittedRF = myRFormula.fit(df)

  val preparedDF = fittedRF.transform(df)
  preparedDF.sample(0.2).show(false)

  val km = new KMeans().setK(3)
  km.explainParams()

  val kModel = km.fit(preparedDF)

  val summary = kModel.summary
  println("Cluster sizes")
  summary.clusterSizes.foreach(println)

  println("Cluster centers")
  kModel.clusterCenters.foreach(println)

  val predictedDF = kModel.transform(preparedDF)


  //show dataframe with these optimal segments
  predictedDF.sample(0.2).show(truncate = false)


  val evaluator = new ClusteringEvaluator()
  val silhouette = evaluator.evaluate(predictedDF)
  println(s"Silhouette with squared evaluation distance - $silhouette")


  //to make it easier for you k will be in range from 2 to 20

  val silhouettes = (2 to 20).map(n => testKMeans(preparedDF, n))

  println("Silhouette scores from 2 to 20 K segments:")
  println(silhouettes.mkString(","))



}
