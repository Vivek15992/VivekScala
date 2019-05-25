package com.spark.learn
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

import org.apache.spark.storage._

object Memory {
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
 val airline_df = spark.sqlContext.read.format("csv")
    .option("header", true)
    .load("C:/Users/pc/workspace/airline.csv")
    
    
    
  val df_mem =  airline_df.persist(StorageLevel.MEMORY_ONLY_SER)
  df_mem.unpersist()
  
    
    
  }
}