package com.scala.teach

object FlatMapEx {
  def main(args:Array[String])
  {
    val fruits = List("apple", "banana", "orange")
    val map_res = fruits.map(_.toUpperCase)
    println(map_res)
    val flat_res = fruits.flatMap(_ .toUpperCase())
    println(flat_res)
    
  }
}