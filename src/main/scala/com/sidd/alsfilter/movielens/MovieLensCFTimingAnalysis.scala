package com.sidd.alsfilter.movielens

import java.io.{File, PrintWriter}
import java.util.Scanner

import com.sidd.alsfilter.matrixlib.DistributedSparseMatrix

/**
  * Get execution times for all combinations of given (comma separated) processors, data fractions, and k's
  *
  * Command line arguments:
  *   ratings file: The location of the MovieLens ratings.dat file
  *   output file: The location to output a CSV of execution times based on parameters
  *   processor numbers: Comma separated value of different processor values to get an analysis for
  *   data fractions: Comma separated value of different fractions of workloads to get an analysis for. Cuts the number of users to analyze.
  *   ks: Comma separated value of different k values to get an analysis for. k is the rank of the decomposed matrices
  */
object MovieLensCFTimingAnalysis {

  def main(args:Array[String]):Unit = {

    // Set up input arguments
    if(args.length != 5){
      throw new Exception("Usage: java -cp [jar] com.sidd.alsfilter.movielens.MovieLensCF " +
        "<ratings file> <output file> <processor attempts> <data fractions> <ks>")
    }
    val Array(ratingsFile,outputFile,processorsCSV,dataFracsCSV, ksCSV) = args
    val numProcessorsArr = processorsCSV.split(",").map(_.toInt)
    val dataFracsArr = dataFracsCSV.split(",").map(_.toDouble)
    val ksArr = ksCSV.split(",").map(_.toInt)

    // output file
    val output = new PrintWriter(new File(outputFile))

    // for each num processor, data frac, and k combination
    for(numProcessors <- numProcessorsArr){
      for(dataFrac <- dataFracsArr){
        for(k <- ksArr) {

          // Data specific information
          val numMovies = 3952
          val numUsers = (6040 * dataFrac).toInt
          val numRatings = 1000209
          val delim = "::"

          // Build the input matrix
          val inMatrix = new DistributedSparseMatrix(numUsers, numMovies, numProcessors, 100)
          val ratingsIn = new Scanner(new File(ratingsFile))
          var nextLine = ratingsIn.nextLine().split(delim)
          while (ratingsIn.hasNext && nextLine(0).toInt <= numUsers) {
            val Array(userStr, movieStr, ratingStr, _) = nextLine
            inMatrix.set(userStr.toInt - 1, movieStr.toInt - 1, ratingStr.toDouble)
            if (ratingsIn.hasNext) nextLine = ratingsIn.nextLine().split(delim)
          }

          // Train the ALS Model and time it to see how long it takes
          val timeStart = System.currentTimeMillis()
          val alsModel = ALS.train(inMatrix, k, 1, 10, numProcessors, 100)
          val time = System.currentTimeMillis() - timeStart
          println(s"Iteration complete with k=$k,numProcessors=$numProcessors,dataFrac=$dataFrac with time=${time}ms")
          output.println(s"$k,$numProcessors,$dataFrac,$time")
        }
      }
    }

    output.close()

  }

  // Map movie IDs to titles
  def buildMovieMap(moviesFile:String,delim:String) = {
    val movieMap = scala.collection.mutable.Map[Int,String]()
    val moviesIn = new Scanner(new File(moviesFile))
    while(moviesIn.hasNext){
      val Array(id,movie,_) = moviesIn.nextLine().split(delim)
      movieMap+=((id.toInt-1,movie))
    }
    movieMap
  }

}
