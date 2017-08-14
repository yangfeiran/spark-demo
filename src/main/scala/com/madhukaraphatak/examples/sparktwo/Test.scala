package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/11.
  */
object Test {
  def main(args: Array[String]): Unit = {
//    val s = Vectors.sparse(5, Seq((1, 1.0), (3, 7.0)));
    import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
    val sparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    val df = sparkSession.createDataFrame(Seq(
      (0, Array("a", "d", "c","b")),
      (1, Array("a", "d", "b", "d", "a","c"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(4)
      .setMinDF(2)
      .fit(df)

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    val cvm = new CountVectorizerModel(Array("a", "b", "c","d"))
      .setInputCol("words")
      .setOutputCol("features")

    cvModel.transform(df).show(false)
    cvm.transform(df).show(false)
  }
}
