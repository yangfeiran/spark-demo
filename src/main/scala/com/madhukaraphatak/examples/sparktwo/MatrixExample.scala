package com.madhukaraphatak.examples.sparktwo

/**
  * Created by Administrator on 2017/8/24.
  */
object MatrixExample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.mllib.linalg.{Matrix, Matrices}

    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
println(dm)
    // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    println(sm)
    val sm1: Matrix = Matrices.sparse(3, 3, Array(0,2,3,6), Array(0, 2,1,0,1,2), Array(1,2,3,4,5,6))
    println(sm1)
  }
}
