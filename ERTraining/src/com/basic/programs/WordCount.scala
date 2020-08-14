package com.basic.programs


import scala.io.Source
import java.io.Writer
import java.io.PrintWriter
import java.io.File
import scala.io.BufferedSource
import com.sun.org.apache.bcel.internal.classfile.LineNumber


object Wordcount {
  
  def main(args:Array[String]) {
   
  def mapLines(x:String): String = {
    
    var lineInTheString = x.replace(","," ")
    
    //println("lineInTheString is:  " + lineInTheString)
    return lineInTheString
  }
  
  def loadData(id:Int,date:String):String = {
    
    var message = "";
    if((id.equals(1) || date.equals("9-06-2019"))){
      message = "it's working"
    }
    else message = "";
     
    return message;
  }
  
  
  val writer = new PrintWriter(new File("C://Users/Venkatesh/Desktop/Write.txt"))
  
  //println(Source.fromFile("C://Users/Venkatesh/Desktop/input/test.csv")) // returns scala.io.BufferedSource non-empty iterator instance
  
  val s1 = Source.fromFile("C://Users/Venkatesh/Desktop/input/test.csv")//.mkString; //returns the file data as String
  //println(s1)

  //splitting String data with white space and calculating the number of occurrence of each word in the file  
 // val counts = s1.replaceAll(",", " ").split("\\s+").groupBy(x=>x).mapValues(x=>x.length)
  
  //println(counts)
  var s2  = List("");
  for(line <- s1.getLines){
    //println(line)
     var s3 = mapLines(line)
     s2 = s3 :: s2
    
  }
  s2.foreach(println)
  
  var listis = List("")
  try{
  for(i <- 0 until s2.length ){
    
    var msg = loadData(s2(i).substring(0,2).toInt,s2(i).substring(3))
    
    listis = msg :: listis
    
}
  } catch{case x: ArithmeticException => {
    println("Arithmetic Exception")
  } case x:NumberFormatException =>{
    println("NumberFormatException")
  } case x:StringIndexOutOfBoundsException => {
    println("StringIndexOutOfBoundsException")
  }
  }
  
  println("printing list")
  listis.foreach(println)
  //println(s2)
  
  //writer.write(s1)
  writer.close()
  

  //var obj = new InsertData("");
  
  //var ss = obj.connection
  
  //println("return message is " + ss)
  
  }

}