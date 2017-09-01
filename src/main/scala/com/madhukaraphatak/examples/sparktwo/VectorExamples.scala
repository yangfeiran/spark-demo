package com.madhukaraphatak.examples.sparktwo

import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
  * Created by Administrator on 2017/8/23.
  */
object VectorExamples {
  def main(args: Array[String]): Unit = {
    val shit = Seq(1,2,0,0)
    val vec1 = Vectors.dense(shit(0),shit(1),shit(2),shit(3));
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
    val sv2: Vector = Vectors.sparse(4, Seq((0, 1.0), (2, 3.0)))
println(vec1)
    println(dv)
    println(sv1)
    println(sv2)
  }
}
