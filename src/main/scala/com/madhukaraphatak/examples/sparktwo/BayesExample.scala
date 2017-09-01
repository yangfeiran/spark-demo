package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/30.
  */
object BayesExample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.classification.NaiveBayes
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
    val sparkSession = SparkSession.builder().master("local").appName("BayesExample").getOrCreate()
    // Load the data stored in LIBSVM format as a DataFrame.
//    val data = sparkSession.read.format("libsvm").load("mllib/sample_libsvm_data.txt")
val simple = sparkSession.createDataFrame(Seq(
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,1),0),
  (Vectors.dense(1,1),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(1,0),0),
  (Vectors.dense(2,1),1),
  (Vectors.dense(2,2),1),
  (Vectors.dense(2,2),1),
  (Vectors.dense(2,2),1),
  (Vectors.dense(2,2),1),
  (Vectors.dense(2,2),1),
  (Vectors.dense(2,2),1),
  (Vectors.dense(2,2),1),
  (Vectors.dense(2,2),1),
  (Vectors.dense(3,2),1),
  (Vectors.dense(3,1),1),
  (Vectors.dense(3,1),1),
  (Vectors.dense(3,2),1),
  (Vectors.dense(3,2),0)
)).toDF("features","label")
    val simple1 = sparkSession.createDataFrame(Seq(
      (Vectors.dense(2,1),1),
      (Vectors.dense(1,0),0),
      (Vectors.dense(3,3),1)
    )).toDF("features","label")
    simple1.show(false)
    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = Array(simple,simple1)

    // Train a NaiveBayes model.
    val model = new NaiveBayes().setFeaturesCol("features").setLabelCol("label").setSmoothing(1)
      .fit(trainingData)

    // Select example rows to display.
    val predictions = model.transform(testData)
    predictions.show(false)

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test set accuracy = " + accuracy)
  }
}
