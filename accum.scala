package com.spark.learn
import org.apache.spark._
import org.apache.spark.sql.functions.{ sum, broadcast, col,lit,udf }
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.log4j._
object accum {
  def main(args:Array[String])
  {
    val sc = SparkSession
      .builder
      .appName("Accumulator")
      .master("local[*]")
      .config("spark.sql.warehouse.dirAccumulatorExample", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
     val accum = sc.sparkContext.longAccumulator("")
     sc.sparkContext.parallelize(Array(1, 2, 3)).foreach(x => accum.add(x))
     println("accum.............."+accum.value)
  }
}