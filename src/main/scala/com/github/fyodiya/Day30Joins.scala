package com.github.fyodiya

import com.github.fyodiya.SparkUtilities.readDataWithView
import org.apache.spark.sql.functions.expr

object Day30Joins extends App {

  println("Chapter 8: Joins!")
  val spark = SparkUtilities.getOrCreateSpark("Joins playground")
  val filePath = "src/resources/retail-data/all/*.csv"
  val df = readDataWithView(spark, filePath)

  //Join Expressions
  //A join brings together two sets of data, the left and the right, by comparing the value of one or
  //more keys of the left and right and evaluating the result of a join expression that determines
  //whether Spark should bring together the left set of data with the right set of data. The most
  //common join expression, an equi-join, compares whether the specified keys in your left and
  //right datasets are equal. If they are equal, Spark will combine the left and right datasets. The
  //opposite is true for keys that do not match; Spark discards the rows that do not have matching
  //keys. Spark also allows for much more sophsticated join policies in addition to equi-joins. We
  //can even use complex types and perform something like checking whether a key exists within an
  //array when you perform a join.
  //Join Types
  //Whereas the join expression determines whether two rows should join, the join type determines
  //what should be in the result set. There are a variety of different join types available in Spark for
  //you to use:
  //Inner joins (keep rows with keys that exist in the left and right datasets)
  //Outer joins (keep rows with keys in either the left or right datasets)
  //Left outer joins (keep rows with keys in the left dataset)
  //Right outer joins (keep rows with keys in the right dataset)
  //Left semi joins (keep the rows in the left, and only the left, dataset where the key
  //appears in the right dataset)
  //Left anti joins (keep the rows in the left, and only the left, dataset where they do not
  //appear in the right dataset)
  //Natural joins (perform a join by implicitly matching the columns between the two
  //datasets with the same names)
  //Cross (or Cartesian) joins (match every row in the left dataset with every row in the
  //right dataset)
  //If you have ever interacted with a relational database system, or even an Excel spreadsheet, the
  //concept of joining different datasets together should not be too abstract. Let’s move on to
  //showing examples of each join type. This will make it easy to understand exactly how you can
  //apply these to your own problems. To do this, let’s create some simple datasets that we can use
  //in our examples:

  import spark.implicits._ //this will let use  toDF on simple sequence
  //regular Sequence doesn't have toDF

  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")
  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")
  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
    .toDF("id", "status")

  person.show()
  graduateProgram.show()
  sparkStatus.show()

  //Next, let’s register these as tables so that we use them throughout the chapter
  person.createOrReplaceTempView("person")
  graduateProgram.createOrReplaceTempView("graduateProgram")
  sparkStatus.createOrReplaceTempView("sparkStatus")

//Inner Joins
  //Inner joins evaluate the keys in both of the DataFrames or tables and include (and join together)
  //only the rows that evaluate to true. In the following example, we join the graduateProgram
  //DataFrame with the person DataFrame to create a new DataFrame:
  val joinExpression = person.col("graduate_program") === graduateProgram.col("id")

  //Keys that do not exist in both DataFrames will not show in the resulting DataFrame. For
  //example, the following expression would result in zero values in the resulting DataFrame:
  val wrongJoinExpression = person.col("name") === graduateProgram.col("school")

  //Inner joins are the default join, so we just need to specify our left DataFrame and join the right in
  //the JOIN expression:
  person.join(graduateProgram, joinExpression).show()

  //same in SQL
  spark.sql(
    """
      |SELECT * FROM person JOIN graduateProgram
      |ON person.graduate_program = graduateProgram.id
      |""".stripMargin)
    .show()

  //We can also specify this explicitly by passing in a third parameter, the joinType:
  var joinType = "inner"

  //in SQL
  println("Inner join:")
  spark.sql(
    """
      |SELECT * FROM person INNER JOIN graduateProgram
      |ON person.graduate_program = graduateProgram.id""".stripMargin)
    .show()

  //We can also specify the type of join explicitly
  //by passing in a third parameter, the joinType:
  person.join(graduateProgram, joinExpression, joinType = "inner").show()

  println("Inner join:")
  //in SQl
  spark.sql(
    """
      |SELECT * FROM person INNER JOIN graduateProgram
      |ON person.graduate_program = graduateProgram.id
      |""".stripMargin)
    .show()

  //Outer Joins
  //Outer joins evaluate the keys in both of the DataFrames or tables and includes (and joins
  //together) the rows that evaluate to true or false. If there is no equivalent row in either the left or
  //right DataFrame, Spark will insert null:

  println("Outer join:")
  joinType = "outer"
  person.join(graduateProgram, joinExpression, joinType).show()
  person.join(graduateProgram, joinExpression, "outer").show()

  println("Outer join:")
  spark.sql(
    """
      |SELECT * FROM person FULL OUTER JOIN graduateProgram
      |ON graduate_program = graduateProgram.id
      |""".stripMargin)
    .show()

  //Left Outer Joins
  //Left outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from
  //the left DataFrame as well as any rows in the right DataFrame that have a match in the left
  //DataFrame. If there is no equivalent row in the right DataFrame, Spark will insert null:

  println("Left outer join:")
  joinType = "left_outer"
  graduateProgram.join(person, joinExpression, joinType).show()

  println("Left outer join:")
  //in SQl
  spark.sql(
    """
      |SELECT * FROM graduateProgram
      |LEFT OUTER JOIN person
      |ON person.graduate_program = graduateProgram.id
      |""".stripMargin)
    .show()

//Right Outer Joins
  //Right outer joins evaluate the keys in both of the DataFrames or tables and includes all rows
  //from the right DataFrame as well as any rows in the left DataFrame that have a match in the right
  //DataFrame. If there is no equivalent row in the left DataFrame, Spark will insert null:

  println("Right outer join:")
  joinType = "right_outer"
  person.join(graduateProgram, joinExpression, joinType).show()

  println("Right outer join:")
  //in SQL
  spark.sql(
    """
      |SELECT * FROM person RIGHT OUTER JOIN graduateProgram
      |ON person.graduate_program = graduateProgram.id
      |""".stripMargin)
    .show()

  //Left Semi Joins
  //Semi joins are a bit of a departure from the other joins. They do not actually include any values
  //from the right DataFrame. They only compare values to see if the value exists in the second
  //DataFrame. If the value does exist, those rows will be kept in the result, even if there are
  //duplicate keys in the left DataFrame. Think of left semi joins as filters on a DataFrame, as
  //opposed to the function of a conventional join:

  println("Left semi join:")
  joinType = "left_semi"
  //notice how we use the same join expression
  //but this time we start with graduate program
  graduateProgram.join(person, joinExpression, joinType).show() //shows only programs with graduates
  graduateProgram.join(person, joinExpression, joinType).show() //show only persons who attended a known university

  val gradProgram2 = graduateProgram.union(Seq(
    (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())
  gradProgram2.createOrReplaceTempView("gradProgram2")

  gradProgram2.join(person, joinExpression, "inner").show()

  println("Left semi join:")
  //in SQl
  spark.sql(
    """
      |SELECT * FROM gradProgram2 LEFT SEMI JOIN person
      |ON gradProgram2.id = person.graduate_program
      |""".stripMargin)
    .show()

  //Left Anti Joins
  //Left anti joins are the opposite of left semi joins. Like left semi joins, they do not actually
  //include any values from the right DataFrame. They only compare values to see if the value exists
  //in the second DataFrame. However, rather than keeping the values that exist in the second
  //DataFrame, they keep only the values that do not have a corresponding key in the second
  //DataFrame. Think of anti joins as a NOT IN SQL-style filter:

  println("Left anti join:")
  joinType = "left_anti"
  graduateProgram.join(person, joinExpression, joinType).show()

  //in SQL
  spark.sql(
    """
      |SELECT * FROM graduateProgram LEFT ANTI JOIN person
      |ON graduateProgram.id = person.graduate_program""".stripMargin)
    .show()

  //Natural Joins
  //Natural joins make implicit guesses at the columns on which you would like to join. It finds
  //matching columns and returns the results. Left, right, and outer natural joins are all supported
//WARNING
  //Implicit is always dangerous! The following query will give us incorrect results because the two
  //DataFrames/tables share a column name (id), but it means different things in the datasets. You should
  //always use this join with caution.

  //in SQL
  println("Natural join:")
  spark.sql(
    """
      |SELECT * FROM graduateProgram NATURAL JOIN person""".stripMargin)
    .show()
//logically incorrect, since id columns have different meanings


  //Cross (Cartesian) Joins
  //The last of our joins are cross-joins or cartesian products. Cross-joins in simplest terms are inner
  //joins that do not specify a predicate. Cross joins will join every single row in the left DataFrame
  //to ever single row in the right DataFrame. This will cause an absolute explosion in the number of
  //rows contained in the resulting DataFrame. If you have 1,000 rows in each DataFrame, the crossjoin of these will result in 1,000,000 (1,000 x 1,000) rows. For this reason, you must very
  //explicitly state that you want a cross-join by using the cross join keyword

  println("Cross (Cartesian) join:")
  joinType = "cross"
  graduateProgram.join(person, joinExpression, joinType).show()

  println("Cross (Cartesian) join:")
  //in SQL
  spark.sql(
    """
      |SELECT * FROM graduateProgram CROSS JOIN person
      |ON graduateProgram.id = person.graduate_program""".stripMargin)
    .show()

  //If you truly intend to have a cross-join, you can call that out explicitly:
  println("Cross join:")
  person.crossJoin(graduateProgram).show()

  println("Cross join:")
  //in SQl
  spark.sql(
    """
      |SELECT * FROM graduateProgram CROSS JOIN person
      |""".stripMargin).show()

  spark.conf.set("spark.sql.crossJoin.enable", value = true)
  gradProgram2.join(person, joinExpression, "cross")
    .show()

  //WARNING
  //You should use cross-joins only if you are absolutely, 100 percent sure that this is the join you need.
  //There is a reason why you need to be explicit when defining a cross-join in Spark. They’re dangerous!
  //Advanced users can set the session-level configuration spark.sql.crossJoin.enable to true in
  //order to allow cross-joins without warnings or without Spark trying to perform another join for you.

//Joins on Complex Types
  //Even though this might seem like a challenge, it’s actually not. Any expression is a valid join
  //expression, assuming that it returns a Boolean

  println("Joins on complex types:")
  person.withColumnRenamed("id", "personId")
    .join(sparkStatus, expr("array_contains(spark_status, id)")).show()

  println("Joins on complex types:")
  //in SQl
  spark.sql(
    """
      |SELECT * FROM
      |(select id as personId, name, graduate_program, spark_status FROM person)
      |INNER JOIN sparkStatus ON array_contains(spark_status, id)""".stripMargin)
    .show()

  //Handling Duplicate Column Names
  //One of the tricky things that come up in joins is dealing with duplicate column names in your
  //results DataFrame. In a DataFrame, each column has a unique ID within Spark’s SQL Engine,
  //Catalyst. This unique ID is purely internal and not something that you can directly reference.
  //This makes it quite difficult to refer to a specific column when you have a DataFrame with
  //duplicate column names.
  //This can occur in two distinct situations:
  //The join expression that you specify does not remove one key from one of the input
  //DataFrames and the keys have the same column name
  //Two columns on which you are not performing the join have the same name
  //Let’s create a problem dataset that we can use to illustrate these problems:

  val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
  val joinExpr = gradProgramDupe.col("graduate_program") === person.col(
    "graduate_program")

  //Note that there are now two graduate_program columns, even though we joined on that key:
  person.join(gradProgramDupe, joinExpr).show()

  //The challenge arises when we refer to one of these columns:
  person.join(gradProgramDupe, joinExpr).select("graduate_program").show()

  //Given the previous code snippet, we will receive an error. In this particular example, Spark
  //generates this message:
    //org.apache.spark.sql.AnalysisException: Reference 'graduate_program' is
    //ambiguous, could be: graduate_program#40, graduate_program#1079.;

  //Approach 1: Different join expression
  //When you have two keys that have the same name, probably the easiest fix is to change the join
  //expression from a Boolean expression to a string or sequence. This automatically removes one of
  //the columns for you during the join:
  person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()

  //Approach 2: Dropping the column after the join
  //Another approach is to drop the offending column after the join. When doing this, we need to
  //refer to the column via the original source DataFrame. We can do this if the join uses the same
  //key names or if the source DataFrames have columns that simply have the same name:
  person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program"))
    .select("graduate_program").show()
  val joinExpr2 = person.col("graduate_program") === graduateProgram.col("id")
  person.join(graduateProgram, joinExpr2).drop(graduateProgram.col("id")).show()
  //This is an artifact of Spark’s SQL analysis process in which an explicitly referenced column will
  //pass analysis because Spark has no need to resolve the column. Notice how the column uses the
  //.col method instead of a column function. That allows us to implicitly specify that column by
  //its specific ID.

  //Approach 3: Renaming a column before the join
  //We can avoid this issue altogether if we rename one of our columns before the join:
  val gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
  val joinExpr3 = person.col("graduate_program") === gradProgram3.col("grad_id")
  person.join(gradProgram3, joinExpr3).show()

  //Conclusion
  //In this chapter, we discussed joins, probably one of the most common use cases. One thing we
  //did not mention but is important to consider is if you partition your data correctly prior to a join,
  //you can end up with much more efficient execution because even if a shuffle is planned, if data
  //from two different DataFrames is already located on the same machine, Spark can avoid the
  //shuffle. Experiment with some of your data and try partitioning beforehand to see if you can
  //notice the increase in speed when performing those joins. In Chapter 9, we will discuss Spark’s
  //data source APIs. There are additional implications when you decide what order joins should
  //occur in. Because some joins act as filters, this can be a low-hanging improvement in your
  //workloads, as you are guaranteed to reduce data exchanged over the network.
  //The next chapter will depart from user manipulation, as we’ve seen in the last several chapters,
  //and touch on reading and writing data using the Structured APIs.

}
