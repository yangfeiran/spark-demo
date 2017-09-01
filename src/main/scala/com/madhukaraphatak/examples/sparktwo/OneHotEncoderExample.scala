package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/25.
  */
object OneHotEncoderExample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
    val sparkSession = SparkSession.builder().master("local").appName("Feature1").getOrCreate()
    val df = sparkSession.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")

    val encoded = encoder.transform(indexed)
    encoded.show()
  }
}
