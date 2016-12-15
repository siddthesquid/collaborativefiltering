package com.sidd.alsfilter.movielens

import java.io.{File, PrintWriter}
import java.util.Scanner

import com.sidd.alsfilter.matrixlib.DistributedSparseMatrix

/**
  * Perform validation of the algorithm by creating a training set and test set
  * of users and removing some movie recommendations from the test set of users to see
  * if they will get rerecommended with a high score. This will output to an external file.
  *
  * Command line arguments:
  *   ratings file: The location of the MovieLens ratings.dat file
  *   output file: The location to output a CSV of movie recommendation ranks and their occurences
  *   training set fraction: what fraction of users belong to the training set
  *   k: rank of ALS matrices (20 is a safe number)
  *   lambda: regularization factor (1 is a safe number)
  *   iter: number of iterations to run ALS for
  */
object MovieLensCFValidation{

  def main(args:Array[String]):Unit = {

    // Set up input arguments
    if(args.length != 6){
      throw new Exception("Usage: java -cp [jar] com.sidd.alsfilter.movielens.MovieLensCFValidation " +
        "<ratings file> <output file> <training set fraction> <k> <lambda> <iterations>")
    }

    val Array(ratingsFile,outFile,dataFracStr,kStr,lambdaStr,iterStr) = args
    val dataFrac = dataFracStr.toDouble
    val numProcessors = 8
    val k = kStr.toInt
    val lambda = lambdaStr.toInt
    val iter = iterStr.toInt

    // Data specific information
    val numMovies = 3952
    val numUsers = 6040
    val numRatings = 1000209
    val delim = "::"
    val trainingSize = (numUsers * dataFrac).toInt
    val testSize = numUsers - trainingSize
    val retention = 0.75

    // Build the input matrix
    val inMatrix = new DistributedSparseMatrix(numUsers,numMovies,numProcessors,100)
    val ratingsIn = new Scanner(new File(ratingsFile))
    var userStr:String = null
    var movieStr:String = null
    var ratingStr:String = null
    do{
      var ratingInfo = ratingsIn.nextLine().split(delim)
      userStr = ratingInfo(0)
      movieStr = ratingInfo(1)
      ratingStr = ratingInfo(2)
      if(userStr.toInt <= trainingSize) inMatrix.set(userStr.toInt-1,movieStr.toInt-1,ratingStr.toDouble)
    } while(ratingsIn.hasNext && (userStr.toInt <= trainingSize))

    // Train the ALS Model
    val alsModel = ALS.train(inMatrix,k,lambda,iter,numProcessors,100,true)

    // Starting to collect rankings of missing movies
    val movieRanks = Array.ofDim[Int](numMovies)
    var oldUser = userStr.toInt
    var nextUser:Int = 0

    // Only do this if there are users beyond the training set
    if(oldUser > trainingSize) {
      var predictM = new DistributedSparseMatrix(1,numMovies,numProcessors,100)

      // We will randomly hold back some movies based on some retention rate. We will look for these movies when
      // we make predictions
      var moviesHeldBack = Set[Int]()
      if(Math.random() < retention) {
        predictM.set(0, movieStr.toInt - 1, ratingStr.toDouble)
      } else {
        moviesHeldBack += (movieStr.toInt - 1)
      }

      //
      while(ratingsIn.hasNext()){
        var ratingInfo = ratingsIn.nextLine().split(delim)
        userStr = ratingInfo(0)
        movieStr = ratingInfo(1)
        ratingStr = ratingInfo(2)
        nextUser = userStr.toInt

        // We are done analyzing the current user. Let us make a prediction for them
        if(oldUser!=nextUser){
          println(s"Predicting for user $oldUser.")
          val predictions = alsModel.predict(predictM,numProcessors,100)
          val finalList = predictions.sortBy(_._2).reverse

          // Where do the movies we removed rank in our collaborative filter?
          for(i <- 0 until numMovies){
            if (moviesHeldBack.contains(finalList(i)._1)){
              movieRanks(i) = movieRanks(i) + 1
            }
          }

          // Reset data structures for the next user
          moviesHeldBack = Set[Int]()
          predictM = new DistributedSparseMatrix(1,numMovies,numProcessors,100)
        }

        // We will randomly hold back some movies based on some retention rate. We will look for these movies when
        // we make predictions
        if(Math.random() < retention) {
          predictM.set(0, movieStr.toInt - 1, ratingStr.toDouble)
        } else {
          moviesHeldBack += (movieStr.toInt - 1)
        }
        oldUser = nextUser
      }

      // For the final user
      val predictions = alsModel.predict(predictM,numProcessors,100)
      val finalList = predictions.sortBy(_._2).reverse
      for(i <- 0 until numMovies){
        if (moviesHeldBack.contains(finalList(i)._1)){
          movieRanks(i) = movieRanks(i) + 1
        }
      }

    }

    // Output results to a file
    val out = new PrintWriter(new File(outFile))
    for(i <- 0 until numMovies){
      out.println(s"${i+1},${movieRanks(i)}")
    }
    out.close()

  }
}
