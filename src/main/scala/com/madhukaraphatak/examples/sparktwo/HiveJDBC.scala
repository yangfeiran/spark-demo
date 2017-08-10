package com.madhukaraphatak.examples.sparktwo

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

/**
  * Created by Administrator on 2017/7/7.
  */
object HiveJDBC {

  Class.forName("org.apache.hive.jdbc.HiveDriver")

  def main(args: Array[String]): Unit = {
    var prop = new Properties
    prop.put("user", "hdfs")
    prop.put("password", "")
    prop.put(JDBCOptions.JDBC_BATCH_FETCH_SIZE, "10")
    prop.put("kerberosAuthType","fromSubject")
    prop.put("driver", "org.apache.hive.jdbc.HiveDriver")
    val con = DriverManager.getConnection("jdbc:hive2://172.16.80.71:10000/default",prop)
    val stat =con.createStatement()
    val res = stat.executeQuery("select * from shit");

    while (res.next()){
      println("======="+res.getString("shit.id"))
    }
  }
}
