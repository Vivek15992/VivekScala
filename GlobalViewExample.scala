package com.spark.learn

import org.apache.spark._
import org.apache.spark.sql._
object GlobalViewExample {
  
  
  def main(rgs:Array[String])
  {
    val sparkSession = SparkSession.builder().appName("Globalview")
    .master("local").getOrCreate()
    
    val carriers_df = sparkSession.sqlContext.read
    .option("header", true).csv("C:/Users/pc/workspace/carriers.csv")
    carriers_df.printSchema()
       
    carriers_df.createGlobalTempView("carriers")
        carriers_df.createOrReplaceTempView("carriers_temp")
        val carriers_Df_temp1 = sparkSession.sql("select * from carriers_temp")
carriers_Df_temp1.printSchema()
    
    val carriers_Df_old = sparkSession.sql("select * from global_temp.carriers")
    println("old session")
    carriers_Df_old.printSchema()
    val newSession = sparkSession.newSession()
    
   
        println("new session")

    val carriers_Df1 = newSession.sql("select * from global_temp.carriers")

        val carriers_Df_temp = newSession.sql("select * from carriers_temp")

    carriers_Df1.printSchema()
    
    
    
  }
}