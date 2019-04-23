
package com.scala.teach

object LearningScala2 {

  def main(args: Array[String]) {
    // Flow control

    // If / else syntax
    if (1 > 3) println("Impossible!")  else println("The world makes sense.")
    //> The world makes sense.

    if (1 < 3) {
      println("Impossible!")
      
       println("Impossible .........!")
    } else {
      println("The world makes sense.")
    } //> The world makes sense.

    // Matching - like switch in other languages:
    val number = 4 //> number  : Int = 3
    number match {
      case 1 => println("One")
      case 2 => println("Two")
      case 3 => println("Three")
      case _ => println("Something else")
    } //> Three

    // For loops
    for (x <- 1 to 4) {
      val squared = x * x
      println(squared)
    } //> 1
    //| 4
    //| 9
    //| 16

    // Until will not print last element
    for (b <- 1 until 10) {
      println(b);
    }

    //  1 2 3 4 5 6 7 8 9

    // for loop with filter
    println("---------------------------")
    println("for loop with filter")
    for (c <- 1 to 10 if c % 2 == 0) {
      println(c);
    }

    // Result: 2  4 6 8 10

    // for loop with yield keyword
    println("---------------------------")
    println("for loop with yield")
    var result = for (a <- 1 to 10) yield a
    for (i <- result) {
      println(i)
    }

    // for loop for iterating collection  
    println("---------------------------")
    println("for loop for iterating collection")

    var list = List(1, 2, 3, 4, 5, 6, 7, 8, 9) // Creating a list
      for (i <- list) {
        println(i)
      }
    // Iterating the list

    // foreach loop for iterating collection
    println("---------------------------")
    println("foreach loop for iterating collection")
    list.foreach(println)

    // for loop using by keyword
    //Element not divided by 2
    println("---------------------------")
    println("for loop using by keyword")
    for (i <- 1 to 10 by 2) {
      println(i)
    }

    // For loop ends

    // While loops
    var x = 10 //> x  : Int = 10
    while (x >= 0) {
      println(x)
      x -= 1
    } //> 10
    //| 9
    //| 8
    //| 7
    //| 6
    //| 5
    //| 4
    //| 3
    //| 2
    //| 1
    //| 0

    x = 0
    do { println(x); x += 1 } while (x <= 10) //> 0
    //| 1
    //| 2
    //| 3
    //| 4
    //| 5
    //| 6
    //| 7
    //| 8
    //| 9
    //| 10

    // Expressions
    // "Returns" the final value in a block automatically

      }
}