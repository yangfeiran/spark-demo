package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/23.
  */
object NormalizerExample {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local").appName("Normalizer").getOrCreate()
    val conf = new SparkConf().setAppName("NormalizerExample")
    val sc = sparkSession.sparkContext

    val dataFrame = sparkSession.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.5, -1.0)),
      (1, Vectors.dense(2.0, 1.0, 1.0)),
      (2, Vectors.dense(4.0, 10.0, 2.0))
    )).toDF("id", "features")

    // Normalize each Vector using $L^1$ norm.
    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val l1NormData = normalizer.transform(dataFrame)
    println("Normalized using L^1 norm")
    l1NormData.show()

    // Normalize each Vector using $L^\infty$ norm.
    val lInfNormData = normalizer.transform(dataFrame, normalizer.p -> Double.PositiveInfinity)
    println("Normalized using L^inf norm")
    lInfNormData.show()

  }
}
