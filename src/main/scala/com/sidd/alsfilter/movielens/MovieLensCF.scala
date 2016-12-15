package com.sidd.alsfilter.movielens

import java.io.File
import java.util.Scanner

import com.sidd.alsfilter.matrixlib.DistributedSparseMatrix

/**
  * Get suggestions for movies based on your own personal ratings from a given ratings file. Refer to data/movies.dat
  * for movies you can get ratings for. Each line in the configuration file should represent a rating delimited by ::.
  * Refer to data/childrenratingsquery.dat for an example.
  *
  * Command line arguments:
  *   movies file: The location of the MovieLens movies.dat file
  *   ratings file: The location of the MovieLens ratings.dat file
  *   query file: The location of a user input vector of movie preferences
  *   num recommendations: The number of recommendations to output at the end
  *   num processors: The number of processors to parallelize this program with
  */
object MovieLensCF {

  def main(args:Array[String]):Unit = {

    // Set up input arguments
    if(args.length != 5){
      throw new Exception("Usage: java -cp [jar] com.sidd.alsfilter.movielens.MovieLensCF " +
        "<movies file> <ratings file> <query file> <num recommendations> <num processors>")
    }

    val Array(moviesFile,ratingsFile,queryFile,numRecommendationsStr,numProcessorsStr) = args
    val numRecommendations = numRecommendationsStr.toInt
    val numProcessors = numProcessorsStr.toInt

    // Data specific information
    val numMovies = 3952
    val numUsers = 6040
    val numRatings = 1000209
    val delim = "::"

    // Build the input matrix
    val inMatrix = new DistributedSparseMatrix(numUsers,numMovies,numProcessors,100)
    val ratingsIn = new Scanner(new File(ratingsFile))
    while(ratingsIn.hasNext){
      val Array(userStr,movieStr,ratingStr,_) = ratingsIn.nextLine().split(delim)
      inMatrix.set(userStr.toInt-1,movieStr.toInt-1,ratingStr.toDouble)
    }

    // Train the ALS Model
    val alsModel = ALS.train(inMatrix,20,1,10,numProcessors,100,true)

    // Create the predictions vector
    val predictM = new DistributedSparseMatrix(1,numMovies,numProcessors,100)
    val pRatings = readRatings(queryFile,delim)
    pRatings.foreach{rating=>predictM.set(0,rating._1-1,rating._2)}

    // Recommend movies to the user and output as movie names as opposed to ids
    val predictions = alsModel.predict(predictM,numProcessors,100)
    val movieMap = buildMovieMap(moviesFile,delim)
    val finalList = predictions.sortBy(_._2).reverse.map({case (id,power) => (movieMap.get(id).getOrElse(id),power)})
    for(i <- 0 until numRecommendations){
      println(finalList(i))
    }
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

  // Read query ratings from a file
  def readRatings(query:String,delim:String):Array[(Int,Int)] = {
    var pRatings = Array[(Int,Int)]()
    val inQuery = new Scanner(new File(query))
    while(inQuery.hasNext()) {
      val Array(movieID, rating) = inQuery.nextLine().split(delim)
      pRatings = pRatings :+ ((movieID.toInt, rating.toInt))
    }
    return pRatings
  }

}
