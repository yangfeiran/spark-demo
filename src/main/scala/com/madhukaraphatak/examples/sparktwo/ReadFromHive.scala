package com.madhukaraphatak.examples.sparktwo

import java.sql.ResultSet
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

/**
  * Created by Administrator on 2017/7/6.
  */
object ReadFromHive {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("Oracle2Hive").getOrCreate()
    val sparkContext = sparkSession.sqlContext
    var prop = new Properties
    prop.put("user", "hdfs")
    prop.put("password", "")
    prop.put(JDBCOptions.JDBC_BATCH_FETCH_SIZE, "10")
    prop.put("kerberosAuthType","fromSubject")
    prop.put("driver", "org.apache.hive.jdbc.HiveDriver")
//    prop.put("resultSetConcurrency", ResultSet.CONCUR_READ_ONLY.toString)
//    prop.put("resultSetType", ResultSet.TYPE_FORWARD_ONLY.toString)
    //    val df = sparkContext.read.format("jdbc").jdbc("jdbc:oracle:thin:stargate/123456@172.16.50.24:1521:dev", "TEST1", prop)
    val df = sparkSession.read.format("jdbc").jdbc("jdbc:hive2://172.16.80.71:10000/default", "shit", prop)
    df.show();
//    prop.put(JDBCOptions.JDBC_CREATE_TABLE_OPTIONS,"true")
//    df.write.jdbc("jdbc:hive2://172.16.80.71:10000/default", "shit_1", prop)
//    print("=================="+df.count())
//    val test = sparkSession.read
//      .option("url", "jdbc:hive2://172.16.80.71:10000/default")
//      .option("user", "hdfs")
//      .option("password", "")
//      .option("dbtable", "abc")
//      .option("driver", "org.apache.hive.jdbc.HiveDriver")
//        .option(JDBCOptions.JDBC_BATCH_FETCH_SIZE, "10")
//      .format("jdbc")
//      .load()
//    test.show()
  }
}
