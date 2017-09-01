package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/25.
  */
object QuantileDiscretizer {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.feature.QuantileDiscretizer
    import org.apache.spark.ml.feature.Bucketizer
    val spark = SparkSession.builder().master("local").appName("QuantileDiscretizer").getOrCreate()
    val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2),(5,Double.NaN))
    val df = spark.createDataFrame(data).toDF("id", "hour")

    val discretizer = new QuantileDiscretizer()
      .setInputCol("hour")
      .setOutputCol("result")
        .setHandleInvalid("keep")
      .setNumBuckets(5).setRelativeError(0)

    val result = discretizer.fit(df).transform(df)
    result.show()
  }
}
