package com.github.fyodiya

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.DataFrame

object Day37Clustering extends App {

  println("Chapter 29: Unsupervised learning, clustering etc.")

  val spark = SparkUtilities.getOrCreateSpark("sparky")
  val filePath = "src/resources/irises/iris.data"

  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .load(filePath)
    .toDF("petal_width", "petal_height", "sepal_width", "sepal_height", "irisType")
  //I could rename immediately instead of using withColumnRenamed below

  //  val flowerDF = df.withColumnRenamed("_c4", "irisType")
  //  flowerDF.show()

  //  flowerDF.describe().show() //check for any inconsistencies, maybe there are some outliers
  //maybe some missing data

  val myRFormula = new RFormula().setFormula("irisType ~ . ")
  //of course we could have use VectorAssembler instead
  //since we do not need category transformed to numeric

  val fittedRF = myRFormula.fit(df)

  val preparedDF = fittedRF.transform(df)
  preparedDF.sample(0.2).show(false)

  //we will not divide in train and test sets here since we assume we have NO categories at all

  val km = new KMeans().setK(3)
    .setFeaturesCol("features") //this is default name
    .setPredictionCol("prediction") //also default name

  println(km.explainParams())

  val kModel = km.fit(preparedDF)

  val summary = kModel.summary
  println("Cluster sizes")
  summary.clusterSizes.foreach(println)

  println("Cluster centers")
  kModel.clusterCenters.foreach(println)

  val predictedDF = kModel.transform(preparedDF)

  predictedDF.sample(0.2).show(truncate = false)
  val evaluator = new ClusteringEvaluator()

  val silhouette = evaluator.evaluate(predictedDF)
  println(s"Silhouette with squared evaluation distance - $silhouette")

  //let's make a little testing function

  def testKMeans(df: DataFrame, k: Int): Double = {
    println(s"Testing K Means with $k clusters")
    val km = new KMeans().setK(k)

    val kmModel = km.fit(df)
    println("Cluster sizes")
    println(kmModel.summary.clusterSizes.mkString(","))

    val predictions = kmModel.transform(df)
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared Euclidean distance = $silhouette")

    silhouette
  }

  //we check our silhouette score from 2 to 8 divisions,
  //our best silhouette score would be with 150 segments since we have 150 rows, but that would be useless...
  val silhouettes = (2 to 8).map(n => testKMeans(preparedDF, n))

  println("Silhouette scores from 2 to 8 K segments:")
  println(silhouettes.mkString(","))

}
