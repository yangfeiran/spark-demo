package ml

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/29.
  */
object FPMining {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.fpm.FPGrowth
    import org.apache.spark.ml._
    val sparkSession = SparkSession.builder().master("local").appName("FPMining").getOrCreate()
    val dataset = sparkSession.createDataFrame(Seq(
      (0,"1 2 5"),
      (1,"1 2 3 5"),
      (2,"1 2")
    )).toDF("id","itemString")
    val t = new Tokenizer().setInputCol("itemString").setOutputCol("items")
val data = t.transform(dataset)
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.5).setMinConfidence(0.6)
    val model = fpgrowth.fit(data)

    // Display frequent itemsets.
    model.freqItemsets.show()

    // Display generated association rules.
    model.associationRules.show()

    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    model.transform(data).show()
  }
}
