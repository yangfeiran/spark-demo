package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/28.
  */
object StringIndexerExample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.feature.StringIndexer
    val sparkSession = SparkSession.builder().master("local").appName("NormalizerExample").getOrCreate()
    val df = sparkSession.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")



    import org.apache.spark.ml.attribute.Attribute
    import org.apache.spark.ml.feature.{IndexToString, StringIndexer}


    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    println(s"Transformed string column '${indexer.getInputCol}' " +
      s"to indexed column '${indexer.getOutputCol}'")
    indexed.show()

    val inputColSchema = indexed.schema(indexer.getOutputCol)
    println(s"StringIndexer will store labels in output column metadata: " +
      s"${Attribute.fromStructField(inputColSchema).toString}\n")

    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")

    val converted = converter.transform(indexed)

    println(s"Transformed indexed column '${converter.getInputCol}' back to original string " +
      s"column '${converter.getOutputCol}' using labels in metadata")
    converted.select("id", "categoryIndex", "originalCategory").show()
  }
}
