package com.spark.learn

import org.apache.spark._
import org.apache.spark.sql.functions.{sum,broadcast,col,lit,udf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.log4j._


object JoinOp {

  def main(args: Array[String]) {

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    
    val airline_df =     spark.sqlContext.read.option("header", true)
    .csv("C:/Users/pc/workspace/airline.csv")
    airline_df.printSchema()
    val carriers_df = spark.sqlContext.read
    .option("header", true).csv("C:/Users/pc/workspace/carriers.csv")
    carriers_df.printSchema()
       
    val finalresult =   airline_df.join((carriers_df),
        airline_df("UNIQUECARRIER") === carriers_df("CODE"),"left")
        
        finalresult.printSchema()
        
        finalresult.show(1)

    val broadcastresult = airline_df.join(broadcast(carriers_df),
        airline_df("UNIQUECARRIER") === carriers_df("CODE"),"left")
    broadcastresult.printSchema()
    
  val carriers_df_renamed = carriers_df
    .withColumnRenamed("CODE", "UNIQUECARRIER")
   
    val result_renamed = airline_df.join(broadcast(carriers_df_renamed),Seq("UNIQUECARRIER"),"left")
    result_renamed.printSchema()
   //Adding New Column 
   val res_newCol=  carriers_df.withColumn("Booking", lit(150))
   .withColumn("Cancellation", lit(15))
   .withColumn("TotalBooking", col("Booking") - col("Cancellation"))
  val divFun = udf(div _)
  val divRes= res_newCol.withColumn("Div",divFun(col("Booking"), col("Cancellation")))
 // divRes.show
 val renamedColumn =divRes.withColumnRenamed("Div", "DivRenamed")
 
renamedColumn.write.mode(SaveMode.Overwrite).format("com.databricks.spark.avro").save("E:/output/newavro")
renamedColumn.write.mode(SaveMode.Overwrite).option("header", true).csv("E:/output/newavro")
renamedColumn.write.mode(SaveMode.Overwrite).parquet("E:/output/newavro")
renamedColumn.write.mode(SaveMode.Overwrite).orc("E:/output/newavro")
val readCSV = spark.sqlContext.read.option("header", true)
    .csv("")
val readParquet = spark.sqlContext.read.parquet("")
val readOrc = spark.sqlContext.read.orc("")
val  readAvro = spark.sqlContext.read.format("com.databricks.spark.avro").load("E:/output/newavro")


 //renamedColumn.printSchema();
  }

  
  def div(a:Int,  b:Int) =
  {
    a/b
  }
}