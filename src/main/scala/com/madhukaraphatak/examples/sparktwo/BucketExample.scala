package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/25.
  */
object BucketExample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.feature.Bucketizer
    val sparkSession = SparkSession.builder().master("local").appName("BucketExample").getOrCreate()
    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)
    val dataFrame = sparkSession.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(dataFrame)

    println(s"Bucketizer output with ${bucketizer.getSplits.length-1} buckets")
    bucketedData.show()
  }
}
