package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.getOrCreateSpark
import org.apache.spark.ml.feature.{StandardScaler, Tokenizer, VectorAssembler}
import org.apache.spark.sql.functions.expr

object Day33PreProcessing extends App {

  println("Day 33: Pre-processing and feature engineering")
  val spark = getOrCreateSpark("Sparky")


  //Any data scientist worth her salt knows that one of the biggest challenges (and time sinks) in
  //advanced analytics is preprocessing. It’s not that it’s particularly complicated programming, but
  //rather that it requires deep knowledge of the data you are working with and an understanding of
  //what your model needs in order to successfully leverage this data. This chapter covers the details
  //of how you can use Spark to perform preprocessing and feature engineering. We’ll walk through
  //the core requirements you’ll need to meet in order to train an MLlib model in terms of how your
  //data is structured. We will then discuss the different tools Spark makes available for performing
  //this kind of work.

  //Formatting Models According to Your Use Case
  //To preprocess data for Spark’s different advanced analytics tools, you must consider your end
  //objective. The following list walks through the requirements for input data structure for each
  //advanced analytics task in MLlib:
  //In the case of most classification and regression algorithms, you want to get your data
  //into a column of type Double to represent the label and a column of type Vector (either
  //dense or sparse) to represent the features.
  //In the case of recommendation, you want to get your data into a column of users, a
  //column of items (say movies or books), and a column of ratings.
  //In the case of unsupervised learning, a column of type Vector (either dense or sparse) is
  //needed to represent the features.
  //In the case of graph analytics, you will want a DataFrame of vertices and a DataFrame
  //of edges.
  //The best way to get your data in these formats is through transformers. Transformers are
  //functions that accept a DataFrame as an argument and return a new DataFrame as a response.
  //This chapter will focus on what transformers are relevant for particular use cases rather than
  //attempting to enumerate every possible transformer.


  //https://stackoverflow.com/questions/44961433/process-csv-from-rest-api-into-spark

  val sales = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/resources/retail-data/by-day/*.csv")
    .coalesce(5)
    .where("Description IS NOT NULL")
  val fakeIntDF = spark.read.parquet("src/resources/simple-ml-integers")
  var simpleDF = spark.read.json("src/resources/simple-ml")
  val scaleDF = spark.read.parquet("src/resources/simple-ml-scaling")

//In addition to this realistic sales data, we’re going to use several simple synthetic datasets as
  //well. FakeIntDF, simpleDF, and scaleDF all have very few rows. This will give you the ability
  //to focus on the exact data manipulation we are performing instead of the various inconsistencies
  //of any particular dataset. Because we’re going to be accessing the sales data a number of times,
  //we’re going to cache it so we can read it efficiently from memory as opposed to reading it from
  //disk every time we need it. Let’s also check out the first several rows of data in order to better
  //understand what’s in the dataset:
  sales.cache()
  sales.show(10, truncate = false)

  //It is important to note that we filtered out null values here. MLlib does not always play nicely with null
  //values at this point in time. This is a frequent cause for problems and errors and a great first step when
  //you are debugging. Improvements are also made with every Spark release to improve algorithm
  //handling of null values.

  //Transformers
  //We discussed transformers in the previous chapter, but it’s worth reviewing them again here.
  //Transformers are functions that convert raw data in some way. This might be to create a new
  //interaction variable (from two other variables), to normalize a column, or to simply turn it into a
  //Double to be input into a model. Transformers are primarily used in preprocessing or feature
  //generation.
  //Spark’s transformer only includes a transform method. This is because it will not change based
  //on the input data.

  //The Tokenizer is an example of a transformer. It tokenizes a string, splitting on a given
  //character, and has nothing to learn from our data; it simply applies a function. We’ll discuss the
  //tokenizer in more depth later in this chapter, but here’s a small code snippet showing how a
  //tokenizer is built to accept the input column, how it transforms the data, and then the output from
  //that transformation:

  val tkn = new Tokenizer().setInputCol("Description")
  tkn.transform(sales.select("Description")).show(false)

//Estimators for Preprocessing
  //Another tool for preprocessing are estimators. An estimator is necessary when a transformation
  //you would like to perform must be initialized with data or information about the input column
  //(often derived by doing a pass over the input column itself). For example, if you wanted to scale
  //the values in our column to have mean zero and unit variance, you would need to perform a pass
  //over the entire data in order to calculate the values you would use to normalize the data to mean
  //zero and unit variance. In effect, an estimator can be a transformer configured according to your
  //particular input data. In simplest terms, you can either blindly apply a transformation (a “regular”
  //transformer type) or perform a transformation based on your data (an estimator type).

  //An example of this type of estimator is the StandardScaler, which scales your input column
  //according to the range of values in that column to have a zero mean and a variance of 1 in each
  //dimension. For that reason it must first perform a pass over the data to create the transformer.
  //Here’s a sample code snippet showing the entire process, as well as the output:

  val ss = new StandardScaler().setInputCol("features")
  ss.fit(scaleDF).transform(scaleDF).show(false)
  //learn or fit how to scale the data, and then scale it (transform)


  //another example
  import spark.implicits._

  //val df = (0 to 20).toDF().selectExpr("value as features")

  //The VectorAssembler is a tool you’ll use in nearly every single pipeline you generate. It helps
  //concatenate all your features into one big vector you can then pass into an estimator. It’s used
  //typically in the last step of a machine learning pipeline and takes as input a number of columns
  //of Boolean, Double, or Vector. This is particularly helpful if you’re going to perform a number
  //of manipulations using a variety of transformers and need to gather all of those results together.
  //The output from the following code snippet will make it clear how this works:
  val df = (0 to 20).toDF()
    .selectExpr("value as f1")
    .withColumn("f2", expr("(f1 + 5) * 10"))
  df.show()

  val va = new VectorAssembler()
    .setInputCols(Array("f1", "f2"))
    .setOutputCol("features") //otherwise we will get a long hash type column name
  val dfAssembled = va.transform(df)
  dfAssembled.show()

  ss.fit(dfAssembled).transform(dfAssembled).show(truncate = false)

  val scalerWithMean = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithStd(true)
    .setWithMean(true ) //default is false

  scalerWithMean.fit(dfAssembled).transform(dfAssembled).show(false)

}
