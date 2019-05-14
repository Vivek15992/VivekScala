package com.spark.learn

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{ SQLContext, DataFrame }
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext

object ParallelizedColl {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
   val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
   import spark.implicits._

    val data = spark.sparkContext.parallelize(Seq(
        ("maths", 52, "selva"),
        ("english", 75, "kumar"),
        ("science", 82, "Bharathi"),
        ("computer", 65, "Suman"),
        ("maths", 85, "Kunal")))
   val res =  data.toDF("sub","marks","name")
   res.printSchema()
 //   val s = data.filter(x => x._1 == "maths")
 val k =  res.filter(res("sub") === "maths")

  k.show()
  }
}