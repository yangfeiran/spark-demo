package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/28.
  */
object PolynomialExpansionExample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.feature.PolynomialExpansion
    import org.apache.spark.ml.linalg.Vectors
    val sparkSession = SparkSession.builder().master("local").appName("PolynomialExpansionExample").getOrCreate()
    val data = Array(
      Vectors.dense(2.0, 1.0),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(3.0, -2.0)
    )
    val df = sparkSession.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val polyExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(2)

    val polyDF = polyExpansion.transform(df)
    polyDF.show(false)
  }
}
