package com.er1099

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Properties

object loadDataIntoMysqlUsingSpark {
     
  System.setProperty("hadoop.home.dir", "C:\\hadoop");
  
  def main(args: Array[String]): Unit = {
    
  

    val conf = new SparkConf().setAppName("PMI Process").setMaster("local[*]").set("spark.executor.memory","1g").set("spark.ui.enabled", "true")

    //val spark = SparkSession.builder().config(conf).getOrCreate();
    val warehouseLocation = "hdfs://localhost:9000/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      //.config("hive.metastore.warehouse.dir", warehouseLocation)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.catalogI mplementation","hive")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
      
      val df = spark.read.option("header","true").csv("hdfs://localhost:9000/user/input/UserData.csv")
      df.show()
      
      val user = "Venkatesh"
      val password = "Venky@453"
      val Jdbcurl = "jdbc:mysql://localhost:3306/myshop"
      val driver = "com.mysql.jdbc.Driver"
      
      val connectionProperties = new Properties
      
      connectionProperties.put("user",user)
      connectionProperties.put("password",password)
      connectionProperties.put("url",Jdbcurl)
      connectionProperties.put("driver",driver)
      connectionProperties.put("dbname","myshop")
      
      Class.forName(driver) 
      
      import org.apache.spark.sql.SaveMode
      val saveMode = SaveMode.Overwrite
      df.write.mode(saveMode).jdbc(url=Jdbcurl,table="users",connectionProperties)
  }   
  
}