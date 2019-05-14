package com.spark.learn

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
    
object DataFrames {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  //  case class Customer(ID:Int, txnId:Int, amount:Float)

  
  def mapper(line:String): Person = {
    val fields = line.split(',')  
    
    val person = Person(fields(0).toInt, fields(1),
        fields(2).toInt, fields(3).toInt)
    return person
  }
  
  
   /* def mapperCustomer(line:String): Customer = {
    val fields = line.split(',')  

    val customer:Customer = Customer(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
    return customer
  }*/
 
  /** Our main function where the action happens */
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
    val lines = spark.sparkContext.textFile("C:/Users/pc/workspace/fakefriends.csv")
      // val Cust_lines = spark.sparkContext.textFile("C:/Users/pc/workspace/customer-orders.csv")

    val people = lines.map(mapper).toDF().cache()
    
   // val cust = Cust_lines.map(mapperCustomer).toDF().cache()
    
  // cust.groupBy(cust("ID")).agg(sum(cust("amount"))).show(500)
    
  //  cust.createGlobalTempView("customer")
    
  //  val res = spark.sql("select sum(amount) from customer groupby id ")
    

    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
    // or sqlContext.table("Hive table name")
    
    println("Here is our inferred schema:")
    people.printSchema()
    
    println("Let's select the name column:")
   val name = people.select("name")
   //.show()
   
   name.show(1)
    
    println("Filter out anyone over 21:")
    val agePeople = people.filter(people("age") < 21)
   agePeople.show(50)
    println("Group by age:")
    people.groupBy("age").agg(avg("numFriends")).show
    
    println("Make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show()
 
    spark.stop()
  }
}