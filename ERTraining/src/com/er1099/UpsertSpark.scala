package com.er1099

import org.apache.spark.sql.SparkSession


object UpsertSpark {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = "hdfs://localhost:9000/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      //.config("hive.metastore.warehouse.dir", warehouseLocation)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.catalogI mplementation", "hive")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

     
    val df = spark.read.option("header", "true").csv("hdfs://localhost:9000/user/input/UserData.csv")
    df.show(10, false)

    import java.sql._
    import java.util.Properties
    import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

    val user = "Venkatesh"
    val Password = "Venky@453"
    val Jdbcurl = "jdbc:mysql://localhost:3306/myshop"
    val driver = "com.mysql.jdbc.Driver"

    val connectionProperties = new Properties

    connectionProperties.put("user", user)
    connectionProperties.put("Password", Password)
    connectionProperties.put("url", Jdbcurl)
    connectionProperties.put("driver", driver)
    connectionProperties.put("dbname", "myshop")

    Class.forName(driver)

    val sc = spark.sparkContext

    val brConnect = sc.broadcast(connectionProperties)

    df.coalesce(10).foreachPartition(partition => {

      val connetionProperties = brConnect.value

      val jdbcUrl = connectionProperties.getProperty("url")
         val user = connectionProperties.getProperty("user")
      val pwd = connectionProperties.getProperty("Password")

      Class.forName(driver)

      val dbc: Connection = DriverManager.getConnection(jdbcUrl, user, pwd)

      val db_batchsize = 100;

      var st: PreparedStatement = null

      partition.grouped(db_batchsize).foreach(batch => {
        batch.foreach {
          row =>
            {
             
              val userNameFieldIndex = row.fieldIndex("UserName")
              val UserName = row.getString(userNameFieldIndex)

              val PasswordFieldIndex = row.fieldIndex("Password")
              val Password = row.getString(PasswordFieldIndex)

              val whereCol: List[String] = List("UserName")

              val sqlString = "SELECT * FROM users where UserName = ?"

              var pstmt: PreparedStatement = dbc.prepareStatement(sqlString)
              pstmt.setString(1, UserName)
              val rs = pstmt.executeQuery()
              var count: Int = 0
              while (rs.next()) {
                print(rs.afterLast())
                count = 1;
              }
              var dmlOprtn = "NULL"

              if (count > 0)
                dmlOprtn = "UPDATE"
              else
                dmlOprtn = "INSERT"
                
              if (dmlOprtn.equals("UPDATE")) {
                val updateSqlString = "update myshop.users set Password = ? where UserName = ?"

                st = dbc.prepareStatement(updateSqlString)

                st.setString(1, Password)
                st.setString(2, UserName)

              } else if (dmlOprtn.equals("INSERT")) {
                val insertSqlString = "INSERT INTO 	myshop.users(UserName,Password) VALUES(?,?)"

                st = dbc.prepareStatement(insertSqlString)
                  
                st.setString(1, UserName)
                st.setString(2, Password)

              }

              st.addBatch()

              println("UserName  ===> " + UserName + " Password ==> " + Password)

            }
            st.executeBatch()
        }
      })
      dbc.close()

    })

  }
}