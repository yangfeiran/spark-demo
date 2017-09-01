package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/2.
  */
object Feature1 {

  import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("Feature1").getOrCreate()
    val sentenceData = sparkSession.createDataFrame(Seq(
      (0.0, "Hi I heard Java Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")



sentenceData.show(false)
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(wordsData)
//    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
//      .setInputCol("words")
//      .setOutputCol("features")

    cvModel.transform(wordsData).show(false)

wordsData.show(false)
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors
    featurizedData.select("rawFeatures").show(false)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show(false)
  }
}