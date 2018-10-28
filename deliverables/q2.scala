// Databricks notebook source
// MAGIC %md
// MAGIC #### Q2 - Skeleton Scala Notebook
// MAGIC This template Scala Notebook is provided to provide a basic setup for reading in / writing out the graph file and help you get started with Scala.  Clicking 'Run All' above will execute all commands in the notebook and output a file 'toygraph.csv'.  See assignment instructions on how to to retrieve this file. You may modify the notebook below the 'Cmd2' block as necessary.
// MAGIC 
// MAGIC #### Precedence of Instruction
// MAGIC The examples provided herein are intended to be more didactic in nature to get you up to speed w/ Scala.  However, should the HW assignment instructions diverge from the content in this notebook, by incident of revision or otherwise, the HW assignment instructions shall always take precedence.  Do not rely solely on the instructions within this notebook as the final authority of the requisite deliverables prior to submitting this assignment.  Usage of this notebook implicitly guarantees that you understand the risks of using this template code. 

// COMMAND ----------

/*
DO NOT MODIFY THIS BLOCK
This assignment can be completely accomplished with the following includes and case class.
Do not modify the %language prefixes, only use Scala code within this notebook.  The auto-grader will check for instances of <%some-other-lang>, e.g., %python
*/
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
case class edges(Source: String, Target: String, Weight: Int)
import spark.implicits._

// COMMAND ----------

/* 
Create an RDD of graph objects from our toygraph.csv file, convert it to a Dataframe
Replace the 'toygraph.csv' below with the name of Q2 graph file.
*/

val df = spark.read.textFile("/FileStore/tables/bitcoinalpha.csv") 
  .map(_.split(","))
  .map(columns => edges(columns(0), columns(1), columns(2).toInt)).toDF()

// COMMAND ----------

// Insert blocks as needed to further process your graph, the division and number of code blocks is at your discretion.
df.count()

// COMMAND ----------

// e.g. eliminate duplicate rows
val noDup = df.dropDuplicates().orderBy("Source")
noDup.show()

// COMMAND ----------

// e.g. filter nodes by edge weight >= supplied threshold in assignment instructions
val thres = noDup.filter("Weight>=5")
thres.show()

// COMMAND ----------

// find node with highest in-degree, if two or more nodes have the same in-degree, report the one with the lowest node id
// find node with highest out-degree, if two or more nodes have the same out-degree, report the one with the lowest node id
// find node with highest total degree, if two or more nodes have the same total degree, report the one with the lowest node id
val out = thres.groupBy("Source").count().toDF("node", "out-degree")
//out.orderBy("node").show()
val in = thres.groupBy("Target").count().toDF("node", "in-degree")
//in.orderBy("node").show()
val in_out = out.join(in, Seq("node"), "outer")
//in_out.show()
val noNulls = in_out.withColumn("out-degree", when($"out-degree".isNull, 0).otherwise($"out-degree")).withColumn("in-degree", when($"in-degree".isNull, 0).otherwise($"in-degree")).withColumn("in-degree", when($"in-degree".isNull, 0).otherwise($"in-degree"))
//noNulls.show()

val columnsToSum = List(noNulls("out-degree"), noNulls("in-degree"))

val output = noNulls.orderBy("node").withColumn("total-degree", columnsToSum.reduce(_ + _)).withColumn("total-degree", when($"total-degree".isNull, 0).otherwise($"total-degree"))
output.show()

val maxIn = output.orderBy(desc("in-degree")).drop("out-degree", "total-degree").limit(1).toDF("v","d")
val maxOut = output.orderBy(desc("out-degree")).drop("in-degree", "total-degree").limit(1).toDF("v","d")
val maxTotal = output.orderBy(desc("total-degree")).drop("in-degree", "out-degree").limit(1).toDF("v","d")
//val stringi = "i"
//maxIn.withColumn("c", stringi).show()
maxIn.toDF.show()
maxOut.show()
maxTotal.show()

// COMMAND ----------

/*
Create a dataframe to store your results
Schema: 3 columns, named: 'v', 'd', 'c' where:
'v' : vertex id
'd' : degree calculation (an integer value.  one row with highest in-degree, a row w/ highest out-degree, a row w/ highest total degree )
'c' : category of degree, containing one of three string values:
                                                'i' : in-degree
                                                'o' : out-degree                                                
                                                't' : total-degree
- Your output should contain exactly three rows.  
- Your output should contain exactly the column order specified.
- The order of rows does not matter.
                                                
A correct output would be:

v,d,c
2,3,i
1,3,o
2,6,t


whereas:
- Node 1 has highest in-degree with a value of 3
- Node 2 has highest out-degree with a value of 3
- Node 2 has highest total degree with a value of 6
*/
//val B = (Seq(3,"i")).toDF("d", "c").as("B")
//val B = Seq(("i")).toDF("c").as("B")
//B.show()

//val someDF = Seq(("i")).toDF("x").as("someDF")
//someDF.show()
//val newDF1 = maxIn.join(B, "full")
//val newDF = maxIn.withColumn("c", someDF("x"))

//newDF.show()


//val other = maxIn.join(maxOut, Seq("v","d"), "full")

val df2 = maxIn.join(maxOut, Seq("v","d"), "full")
val df3 = df2.join(maxTotal, Seq("v","d"), "full")


df2.show()
df3.show()


// COMMAND ----------


display(df3)

// COMMAND ----------

df3.printSchema()

// COMMAND ----------

df3.dropDuplicates().show()
df3.dropDuplicates().count()
