package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/28.
  */
object NGramExample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.feature.NGram
    val sparkSession = SparkSession.builder().master("local").appName("NGramExample").getOrCreate()
    val wordDataFrame = sparkSession.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("id", "words")

    val ngram = new NGram().setN(6).setInputCol("words").setOutputCol("ngrams")

    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.select("ngrams").show(false)
  }
}
