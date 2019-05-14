package com.scala.teach

class MainCompanionExample {
   def sayHelloWorld() {
        println("Hello World");
    }
   
   def display(name:String)
   {
     println("it will display" +name)
   }
    private val ku = 20
}

object MainCompanionExample {
    def sayHi() {
        println("Hi!");
    }
    
    def main(args: Array[String])
    {
      MainCompanionExample.sayHi()
      
      var selva= new MainCompanionExample();
      var selva1= new MainCompanionExample();
       selva1.display("kumar")
      selva.sayHelloWorld();
      selva.display("selva")
      sayHi()
      println("i  :" + selva.ku)

    }
                
    
}