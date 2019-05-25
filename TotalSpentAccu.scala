package com.spark.learn

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word appears in a book as simply as possible. */
object TotalSpentAccu{

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalSpentAccu")
    val accum = sc.longAccumulator("AccumulatorExample")
    // Read each line of my book into an RDD
   var inp = 0
    val input = sc.textFile("C:/Users/pc/workspace/customer-orders.csv")
    input.foreach(line => accum.add(1))
       input.foreach(line => inp + 1)
    printf("Total Lines: %d", accum.value)
      printf("Total Lines 2: %d", inp)

  }

}

