package com.er1099

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Sample {
     
  System.setProperty("hadoop.home.dir", "C:\\hadoop");

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
      
      val df = spark.read.csv("hdfs://localhost:9000/user/input/UserData.csv")
      df.show()
      
      
  
}