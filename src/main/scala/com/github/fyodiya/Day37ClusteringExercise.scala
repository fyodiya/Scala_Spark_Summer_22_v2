package com.github.fyodiya

import com.github.fyodiya.Day37Clustering.testKMeans
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{RFormula, VectorAssembler}

object Day37ClusteringExercise extends App {


  //Find optimal number of segments in the src/resources/csv/cluster_me.csv file
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


  val newFilePath = "src/resources/csv/cluster_me.csv"

  val originalDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .load(newFilePath)
    .toDF("col1", "col2")

  val myVector = new VectorAssembler()
    .setInputCols(Array("col1", "col2"))
    .setOutputCol("features")

  val clusterDF = myVector.transform(originalDF)

  println()
  println("******* New clustering data *******")
  println()

  val minCluster = 2
  val maxCluster = 20
  val newSilhouettes = (minCluster to maxCluster).map(n => testKMeans(clusterDF, n))
  println("Silhouette scores from 2 to 20 K segments")
  println(newSilhouettes.mkString(","))

  val bestSilhouette = newSilhouettes.max
  val bestNumClusters = newSilhouettes.indexOf(bestSilhouette) + minCluster

  println()
  println(s"******* The best number of clusters is ${bestNumClusters} with silhouette $bestSilhouette *******")
  println()

  val kMean = new KMeans().setK(bestNumClusters)
  val clusteredDF = kMean.fit(clusterDF).transform(clusterDF)

  println("Clustered DF:")
  clusterDF.show(false)

  clusteredDF.show(20, truncate = false)

  //here we cans ee that the silhouette have worked perfectly as the max was indeed the best option
  //with real data you might need to play around with a couple of highest scores - remember the elbow method

}
