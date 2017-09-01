package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/28.
  */
object PCAExample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.feature.PCA
    import org.apache.spark.ml.linalg.Vectors
    val sparkSession = SparkSession.builder().master("local").appName("PCAExample").getOrCreate()
    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = sparkSession.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df)
    println(pca.explainParams())
    val result = pca.transform(df).select("pcaFeatures")
    result.show(false)
    val it = pca.pc.colIter

    println(it.toList)
    println(pca.explainedVariance)
  }
}
