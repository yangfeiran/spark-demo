package com.madhukaraphatak.examples.sparktwo

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
//import com.databricks.spark.avro._
import java.sql.{Connection, DriverManager}
/**
  * Created by Administrator on 2017/5/10.
  */
object Oracle2Hive {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("Oracle2Hive").getOrCreate()
//    val avro = sparkSession.catalog.createExternalTable("external_table_1", "C:\\Users\\Administrator\\Documents\\part-m-00000.avro","com.databricks.spark.avro")
//    val av = sparkSession.read.avro("C:\\Users\\Administrator\\Documents\\part-m-00000.avro")

    import java.util.Properties
    import org.apache.spark.sql.{SaveMode, SparkSession}
    import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
    import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
//    import com.databricks.spark.avro._
//    sparkSession.read.avro("/tmp/clobtest_22").write.saveAsTable("blobtest_22")
    val sparkContext = sparkSession.sqlContext
    var prop =  new Properties
    prop.put("user", "root")
    prop.put("password", "654321")
    prop.put(JDBCOptions.JDBC_BATCH_FETCH_SIZE,"10")
    prop.put("driver","com.mysql.jdbc.Driver")
//    prop.put("driver","oracle.jdbc.driver.OracleDriver")
    prop.put("resultSetConcurrency",ResultSet.CONCUR_READ_ONLY.toString)
    prop.put("resultSetType",ResultSet.TYPE_FORWARD_ONLY.toString)
    //    val df = sparkContext.read.format("jdbc").jdbc("jdbc:oracle:thin:stargate/123456@172.16.50.24:1521:dev", "TEST1", prop)
    val df = sparkContext.read.format("jdbc").jdbc("jdbc:mysql://172.16.50.22:3306/stargate_test", "age_gmy", prop)
    df.show()
//    val map = new Map
    df.write.mode(SaveMode.Overwrite).saveAsTable("oomtest_4")
    val structType = df.schema
    val a = structType.fields
    val new_st_array =  a filter (s => s.dataType!=BinaryType)
    val blob_name =  (a filter (s => s.dataType==BinaryType))(0).name
    val new_st = StructType(new_st_array)
    Class.forName("oracle.jdbc.driver.OracleDriver")
    val connection = DriverManager.getConnection("jdbc:oracle:thin:@172.16.50.24:1521:dev", "stargate", "123456")
    createTable(new_st, "jdbc:oracle:thin:@172.16.50.24:1521:dev", "blobtest_1", "", connection,blob_name)
    df.write.mode(SaveMode.Append).format("jdbc").jdbc("jdbc:oracle:thin:stargate/123456@172.16.50.24:1521:dev", "TEST1", new Properties)
  }

  def createTable(
                   schema: StructType,
                   url: String,
                   table: String,
                   createTableOptions: String,
                   conn: Connection, blobname: String): Unit = {
    schema.toList
    val strSchema = JdbcUtils.schemaString(schema, url)
    // Create the table if the table does not exist.
    // To allow certain options to append when create a new table, which can be
    // table_options or partition_options.
    // E.g., "CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    val sql = s"CREATE TABLE $table ($strSchema $blobname blob) $createTableOptions"
    val statement = conn.createStatement
    try {
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }
  }

}
