package com.spark.learn
import org.apache.spark._
import org.apache.spark.sql.functions.{sum,broadcast,col}
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.log4j._
import scala.util.{ Failure, Success, Try }
import org.apache.spark.storage._
object Repartition {

  def main(args: Array[String]) {

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    

    val airline_df = spark.sqlContext.read.format("csv")
    .option("header", true)
    .load("C:/Users/pc/workspace/airline.csv")
    
    println("getNumPartitions Default:"
        +airline_df.rdd.getNumPartitions)
    //
    val airline_df_ChangePart = spark.sqlContext.read
    .format("csv").option("header", true)
    .load("C:/Users/pc/workspace/airline.csv").repartition(20)
    println("airline_df_ChangePart_Partition"
        +airline_df_ChangePart.rdd.getNumPartitions)
   
  
    val airline_df_repart_col = spark.sqlContext.read.format("csv")
    .option("header", true)
    .load("C:/Users/pc/workspace/airline.csv")
    //Find The count 
    val count = airline_df_repart_col
     .select((airline_df_repart_col("FlightNum"))).distinct().count
  println(count)
    //Repartition
     val repart_df =airline_df_repart_col
     .repartition(airline_df_repart_col("Year"),
         airline_df_repart_col("Month"),
         airline_df_repart_col("FlightNum"))
     /*,
         airline_df_repart_col("Month"),
       airline_df_repart_col("FlightNum")*/
 // println(repart_df.rdd.partitions.length)
  /*airline_df_repart_col.unpersist()
  repart_df.coalesce(10).write.mode(SaveMode.Overwrite)
  .option("header", true).csv("")*/
  
      val s =repart_df.rdd
      .mapPartitions(iter => Array(iter.size).iterator, true) 
   s.foreach(println)
   /* //.get(0).size()
     val count = airline_df.select((airline_df("FlightNum"))).distinct().count
     val tailcount = airline_df.select((airline_df("TailNum"))).distinct().count

    println("Distinct Elem :"+count)
    println("tailcount size"+tailcount)
    println("repart_df Size:"+repart_df.rdd.partitions.size)
 //   airline_df.printSchema() 
    
    repart_df.write.mode(SaveMode.Overwrite).format("com.databricks.spark.avro").save("E:/output/newavro")
   // repart_df.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.avro").save("E:/output/newavroone")

 
  */

}
}