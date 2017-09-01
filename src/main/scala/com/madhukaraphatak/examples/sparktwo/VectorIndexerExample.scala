package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/28.
  */
object VectorIndexerExample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.feature.VectorIndexer
    val sparkSession = SparkSession.builder().master("local").appName("VectorIndexerExample").getOrCreate()
    val data = sparkSession.read.format("libsvm").load("mllib/sample_libsvm_data.txt")

    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)

    val indexerModel = indexer.fit(data)

    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} categorical features: " +
      categoricalFeatures.mkString(", "))

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)
    indexedData.show(false)
  }
}
