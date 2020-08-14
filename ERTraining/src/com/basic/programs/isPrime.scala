package com.basic.programs

object isPrime {
  
  def main(args: Array[String]): Unit = {
    val i  = List(7,8,56,1230654544,565,29);
   i.map( x => isPrime(x) ) 
    
    
  }
  
  def isPrime(i: Int): String = {
    var msg=""
    if (i <= 1)
         msg = "not a prime number" 
    else if (i == 2)
         msg = "not a prime number" 
    else
        if(!(2 until i).exists(n => i % n == 0)) {
           msg = " is a prime number" 
          
        }else  msg = "not a prime number" 
        println(i + " " + msg)
  return msg      
  }
}