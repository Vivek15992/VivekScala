package com.spark.learn
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer
import scala.xml.XML
import org.apache.spark.sql.{ DataFrame, Row, SQLContext, DataFrameReader }
import org.apache.spark.sql.functions._
object JsonExample {
  
  def main(args:Array[String])
  {
   
    
        Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("xmlexample")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
    val josnDF =  spark.read.json("C:/Users/hp/Downloads/spark-examples-master/spark-sql-examples/src/main/resources/zipcodes.json").toDF()
      josnDF.show
  }
}