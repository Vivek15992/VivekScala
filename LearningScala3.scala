package com.scala.teach
object LearningScala3 {
    def main(args:Array[String]){

  // Functions
  
  // Format is def <function name>(parameter name: type...) : return type = { expression }
  // Don't forget the = before the expression!
      

  def squareIt(x: Int)  = {
  	x * x
  }                                               //> squareIt: (x: Int)Int
  
  def cubeIt(x: Int): Int = {x * x * x}           //> cubeIt: (x: Int)Int
  
  println(squareIt(2))                            //> 4
  
  println(cubeIt(2))                              //> 8
  
  // Functions can take other functions as parameters -> HOF
  
  def transformInt(x: Int, f: Int => Int) : Int = {
  	f(x) 
  }                                               //> transformInt: (x: Int, f: Int => Int)Int
  
  def transformInt1(x: Int,y:Int, f: (Int,Int) => Int) : Int = {
  	f(x,y)
  } 
  val result = transformInt(2, cubeIt)            //> result  : Int = 8
  println (result)                                //> 8
  
  // "Lambda functions", "anonymous functions", "function literals"
  // You can declare functions inline without even giving them a name
  // This happens a lot in Spark.
  transformInt(3, x => x * x * x)                 //> res0: Int = 27
  
  transformInt(10, x => x / 2)                    //> res1: Int = 5
  
  transformInt(2, x => {val y = x * 2; y * y})    //> res2: Int = 16
  
  // This is really important!
   def add(a:Int)(b:Int) = {  
        a+b  
    } 
  // Currying Function
  // break down a function that take multiple args into series of function
        var result_curr = add(10)(10)  
        println("result_curr "+result_curr)  
        var addIt = add(10)_ 
        println("addIt :"+addIt)
        var result2_Curr = addIt(3)  
        println("10 + 3 = "+result2_Curr) 
 }
   
}