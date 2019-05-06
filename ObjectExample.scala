

package com.scala.teach

object ObjectExample {
  
  def main(args:Array[String])
  {
      val a = args(0).toInt
      val b = args(1).toInt
          
           println("a :"+ a  +"  b :"+b)
        val result = add(a,b)
        println(result)
   
 //   println("Value1  :"+ a.isInstanceOf[String])
  //  println("Value2 :"+ a.isInstanceOf[Int])
   // println("Value1 + Value2 :"+ value1 + value2)

  }
  
   def add(a:Int ,b:Int) :Integer =
  {
     //print("Inside method" + (a+b))
   val c = a+b
   c
  }
}