package com.scala.teach
object LearningScala4 {
  
    def main(args:Array[String]){

  // Data structures
  
  // Tuples (Also really common with Spark!!)
  // Immutable lists
  // Often thought of as database fields, or columns.
  // Useful for passing around entire rows of data.
   //A tuple groups together simple logical collections of 
      //items without using a class.

  
  val captainStuff = ("Picard", "Enterprise-D", "NCC-1701-D")
   //> captainStuff  : (String, String, String) = (Picard,Enterprise-D,NCC-1701-D)
                                                  //| 
  println(captainStuff)                           //> (Picard,Enterprise-D,NCC-1701-D)
  
  // You refer to individual fields with their ONE-BASED index:
  println(captainStuff._1)                        //> Picard
  println(captainStuff._2)                        //> Enterprise-D
  println(captainStuff._3)                        //> NCC-1701-D
 
 // You can create a key/value pair with ->
 val picardsShip = (("Picard" -> "Enterprise-D"),("Janeway"->"Voyager"))     
 
 //> picardsShip  : (String, String) = (Picard,Enterprise-D)
 println(picardsShip._2._1)                          //> Enterprise-D
 
 // You can mix different types in a tuple
 val aBunchOfStuff = ("Kirk", 1964, true)         //> aBunchOfStuff  : (String, Int, Boolean) = (Kirk,1964,true)
 
 
 
 // Lists
 // Like a tuple, but it's an actual Collection object that has more functionality.
 // Also, it cannot hold items of different types.
 // It's a singly-linked list under the hood.
 
 val shipList = List("Enterprise", "Defiant", "Voyager",
     "Deep Space Nine")
                                               //> shipList  : List[String] = List(Enterprise, Defiant, Voyager, Deep Space Nin
                                                  //| e)
 
 // Access individual members using () with ZERO-BASED index (confused yet?)
 println(shipList(1))                             //> Defiant
 
 // head and tail give you the first item, and the remaining ones.
 println(shipList.head)                           //> Enterprise
 println(shipList.tail)                           //> List(Defiant, Voyager, Deep Space Nine)
 
 // Iterating though a list
 for (ship <- shipList) {println(ship)}           //> Enterprise
                                                  //| Defiant
                                                  //| Voyager
      shipList.foreach(println)                                            //| Deep Space Nine
                                                  
// reduce() can be used to combine together all the items in a collection using some function.
val numberList = List(1, 2, 3, 4, 5)              //> numberList  : List[Int] = List(1, 2, 3, 4, 5)
val sum = numberList.reduce( (x: Int, y: Int) => x + y)
val sum1 = numberList.reduce(_ + _)
                                                  //> sum  : Int = 15
println(sum)                                      //> 15

// filter() can remove stuff you don't want. Here we'll introduce wildcard syntax while we're at it.
val iHateFives = numberList.filter( (x: Int) => x != 5)
                                                  //> iHateFives  : List[Int] = List(1, 2, 3, 4)
val iHateThrees = numberList.filter(_ != 3)       //> iHateThrees  : List[Int] = List(1, 2, 4, 5)

// Note that Spark has its own map, reduce, and filter functions that can distribute these operations. But they work the same way!
// Also, you understand MapReduce now :)

// Concatenating lists
val moreNumbers = List(6, 7, 8)                   //> moreNumbers  : List[Int] = List(6, 7, 8)
val lotsOfNumbers = numberList ++ moreNumbers     //> lotsOfNumbers  : List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8)

// More list fun
val reversed = numberList.reverse                 //> reversed  : List[Int] = List(5, 4, 3, 2, 1)
val sorted = reversed.sorted                      //> sorted  : List[Int] = List(1, 2, 3, 4, 5)
val lotsOfDuplicates = numberList ++ numberList   //> lotsOfDuplicates  : List[Int] = List(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
val distinctValues = lotsOfDuplicates.distinct    //> distinctValues  : List[Int] = List(1, 2, 3, 4, 5)
val maxValue = numberList.max                     //> maxValue  : Int = 5
val total = numberList.sum                        //> total  : Int = 15
val hasThree = iHateThrees.contains(3)            //> hasThree  : Boolean = false

//Map Fun
val numbers = List(1, 2, 3, 4)
val numMul = numbers.map((i: Int) => i * 2)



val listOfAdd = List(2,4,5)
  def timesTwo(i: Int): Int = i * 2
listOfAdd.map(timesTwo)

//(i: Int): Int = i * 2



// Flatten func
val flatRes = List(List(1, 2), List(3, 4)).flatten

//Flat Map Function
//flatMap is a frequently used combinator that combines mapping and flattening. flatMap takes a function that works on the nested lists and then 
//concatenates the results back together.
val nestedNumbers = List(List(1, 2), List(3, 4))
val resFlat = nestedNumbers.flatMap(x => x.map( x => (x* 2)))
println(resFlat)

nestedNumbers.map(x => x.map(_ * 2)).flatten

var newList = List(1,2,3,5,6,78)
newList = newList ++ List(10)
println(newList)
//Arrays are mutable

val numbersArray = Array(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
println("numbersArray"+numbersArray)
 numbersArray ++ Array(8)

// Maps
// Useful for key/value lookups on distinct keys
// Like dictionaries in other languages

 
val shipMap  = Map(1 -> "Enterprise",
    2 -> "Enterprise",
    3 -> "Deep Space Nine",
    4 -> "Voyager")
                                                  //> shipMap  : scala.collection.immutable.Map[String,String] = Map(Kirk -> Ente
                                                  //| rprise, Picard -> Enterprise-D, Sisko -> Deep Space Nine, Janeway -> Voyage
                                                  //| r)
println(shipMap(4))                       //> Voyager

// Dealing with missing keys
println(shipMap.contains(5))               //> false

val archersShip = util.Try(shipMap(6)) getOrElse 1
                                                  //> archersShip  : String = Unknown
println(archersShip)                              //> Unknown
val archers_defualt = util.Try(shipMap(6)) getOrElse (10)
shipMap.get(3)
val archers = util.Try(shipMap(7)) getOrElse (throw new RuntimeException("KEY NOT FOUND IN SHIP_MAP"))

val setEle = Set(1,2,4,4)


    }

}