package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession

/**
  * Logical Plans for Dataframe and Dataset
  */
object DatasetVsDataFrame {

  case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)

  case class Sample(features: SparseVector)

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    import sparkSession.implicits._


    //read data from text file
    val df = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/sales.csv")
    val ds = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/sales.csv").as[Sales]
    val dataset = sparkSession.read.format("libsvm").load("src/main/resources/sample_kmeans_data.txt")
    //    val yang = df.foreach(f=>f.getAs[Int]("transactionId"))
    val yang = df.flatMap(f => {
      val s = Vector(f.getAs[Int]("transactionId"), f.getAs[Int]("customerId"), f.getAs[Int]("itemId"), f.getAs[Int]("amountPaid"))
      s.slice(0,3)
    }
    )
    yang.show()
    dataset.show()
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(yang)
    val WSSSE = model.computeCost(yang)
    println(s"Within Set Sum of Squared Errors = $WSSSE")
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)


    val selectedDF = df.select("itemId")
    val selectedDS = ds.map(_.itemId)

    println("111" + selectedDF.queryExecution.optimizedPlan.numberedTreeString)

    println("222" + selectedDS.queryExecution.optimizedPlan.numberedTreeString)


  }

}
