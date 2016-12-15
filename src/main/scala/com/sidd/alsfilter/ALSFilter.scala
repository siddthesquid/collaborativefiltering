package com.sidd.alsfilter

import com.sidd.alsfilter.matrixlib.{DistributedDenseMatrix, DistributedMatrix, DistributedSparseMatrix, Matrix}

/**
  * An ALS Model that can be used to make predictions
  *
  * @param Y a k-rank product matrix
  * @param k the rank of Y
  * @param lambda the regularization parameter
  */
class ALS(private val Y:Matrix,private val k:Int, private val lambda:Double) {

  /**
    * With this model, predict products to a user, given their current observations
    *
    * @param p the input user vector
    * @param numProcessors the number of processors
    * @param blockSize the size of each block
    * @return an array mapping the product ID to a prediction score
    */
  def predict(p: DistributedSparseMatrix,numProcessors:Int,blockSize:Int): Array[Tuple2[Int, Double]] = {

    // Basically a single iteration of ALS
    val X = ((Y.transpose * Y) + (DistributedDenseMatrix.eye(k,numProcessors,blockSize) * lambda)).inverse * Y.transpose * p.transpose()

    // Predictions are based on the dot product of each product vector and the user vector
    val predictions = Y * X

    // Output each ID mapped to its prediction score. The higher, the better
    var predictionsArr = Array[(Int,Double)]()
    for(i <- 0 until predictions.getRows){
      predictionsArr = predictionsArr :+ ((i,predictions.get(i,0)))
    }
    predictionsArr
  }

}

/**
  * Create an ALS model given the dataset
  */
object ALS {

  /**
    * Train an ALS Model
    *
    * @param input a sparse matrix of all observations
    * @param k the requested rank of the factorized matrices in the model. The smaller it is, the more space is saved
    * @param lambda the regularization parameter
    * @param iter the number of iterations to perform ALS
    * @param numProcessors the number of processors that can be used
    * @param blockSize the size of blocks for block related operations
    * @param debug set to true to print everytime an iteration has completed
    * @return an ALS Model
    */
  def train(input:DistributedSparseMatrix,k:Int,lambda:Double,iter:Int,numProcessors:Int,blockSize:Int,debug:Boolean = false):ALS = {

    // Because the input matrix does not change, it makes sense to precompute the input's transpose
    val inputT = input.transpose(numProcessors,blockSize)

    // X will be randomized to start off with, but Y will be calculated so it can be set to null
    var X:DistributedMatrix = DistributedDenseMatrix.random(input.getRows,k,1,numProcessors,blockSize)
    var Y:DistributedMatrix = null

    for(i <- 1 to iter){

      // Alternate calculating X and Y. Both will be calculated with each iteration.
      Y = (((X.transpose * X) + (DistributedDenseMatrix.eye(k,numProcessors,blockSize) * lambda)).inverse * (X.transpose)  * input).transpose
      X = (((Y.transpose * Y) + (DistributedDenseMatrix.eye(k,numProcessors,blockSize) * lambda)).inverse * (Y.transpose)  * inputT).transpose

      // If debug is turned on, we can get a better idea of when a program will finish
      if(debug) println(s"Finished iteration $i of $iter")
    }

    // Create a new ALS model
    new ALS(Y,k,lambda)

  }

}

/*
val input = inMatrix
val k = 20
val lambda = 1
val iter = 10
val numProcessors = 16
val blockSize = 100
val debug = true
 */