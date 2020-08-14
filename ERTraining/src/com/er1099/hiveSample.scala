package com.er1099

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object hiveSample {
  def main(args: Array[String]): Unit = {
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
    
     var y = 0; 
    
    val ssm1 = new ssm(y)
    //val textFile = spark.read.text("hdfs://localhost:9000/user/venky/wordcount.txt");
   /* textFile.createOrReplaceTempView("textFile")
    val df = spark.table("textFile")
    df.write.saveAsTable("hive_records")
     textFile.show();*/
    //val s = textFile.rdd.map(_.mkString(","))
    
    //val b =s.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey(_ + _).sortByKey(true,4)
    //b.collect().foreach(println)  
    var s = List(1,2,3,4) 
    var e = s.map(x => ssm1.data(x))
    //textFile.write.csv("hdfs://localhost:9000/user/hive/file1"); 
   
   e.foreach(println)
   println("total count is " + y)
  
   
  }
}