package com.basic.programs

import java.sql.{Connection,DriverManager}

class InsertData(x:String)  {
  println("Insert data class called")
  
  val url = "jdbc:mysql://localhost:3306/myshop"
    val driver = "com.mysql.jdbc.Driver"
    val username = "Venkatesh"
    val password = "Venky@453"
    var connection:Connection = _
    try {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
        val statement = connection.createStatement
        val rs = statement.executeQuery("SELECT UserName,Password  FROM users")
        while (rs.next) {
            val UserName = rs.getString("UserName")
            val Password = rs.getString("Password")
            println("UserName = %s, Password = %s".format(UserName,Password))
        }
    } catch {
        case e: Exception => e.printStackTrace
    }
    connection.close
    
    def Connection(x:String):String = {
      println("Connection function called")
      return "sucessfull";
    }
    
}