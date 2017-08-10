package com.madhukaraphatak.examples.sparktwo

import java.io.{File, FileInputStream, FileOutputStream}
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.spark.sql.execution.datasources.FileFormatWriter
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.datanucleus.store.rdbms.JDBCUtils

/**
  * Created by Administrator on 2017/5/9.
  */
object OracleJDBC {
  def main(args: Array[String]): Unit = {
    Class.forName("oracle.jdbc.driver.OracleDriver")
    val connection = DriverManager.getConnection("jdbc:oracle:thin:@172.16.50.24:1521:dev", "stargate", "123456")
//        insertblob(connection)
//    selectblob(connection)

    connection.close()
  }

  def insertblob(connection: Connection): Unit = {
    val insql = "INSERT INTO STARGATE.TEST1 (COL2) VALUES(?)"
    val fname = "C:\\Users\\Administrator\\Desktop\\1.png"
    val f = new File(fname)
    val fis = new FileInputStream(f)
    var pstmt: PreparedStatement = null

    try {
      pstmt = connection.prepareStatement(insql)
      pstmt.setBlob(1, fis)
      pstmt.execute()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      if (pstmt != null) {
        pstmt.close()
      }
      fis.close()
    }
  }

  def selectblob(connection: Connection): Unit = {
    val insql = "select COL2 from YANGFEIRAN.TEST1 where ID = ?"
    val fname = "C:\\Users\\Administrator\\Desktop\\2.png"
    var pstmt: PreparedStatement = null
    val fout = new FileOutputStream(fname)
    try {
      pstmt = connection.prepareStatement(insql)
      pstmt.setInt(1, 2)
      val result = pstmt.executeQuery()

      while (result.next()) {
        val blob = result.getBlob(1)
        val in = blob.getBinaryStream()
        var length = blob.length()
        val buffer = Array[Byte]()
        while ( in.read(buffer) != -1) {
          fout.write(buffer,0,length.toInt)
        }
      }
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    finally {
      if (pstmt != null) {
        pstmt.close()
      }
      fout.close()
    }
  }

}
