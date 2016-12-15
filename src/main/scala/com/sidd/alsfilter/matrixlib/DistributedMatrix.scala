package com.sidd.alsfilter.matrixlib

import java.util.concurrent.atomic.AtomicInteger

/**
  * Abstract implementation of Matrix that allows for distributed operations accross a given number of processors
  *
  * @param rows number of rows in the matrix
  * @param cols number of columns in the matrix
  * @param defNumProcessors default number of processors to parallelize operations over
  * @param defBlocksize default size of (square) blocks in matrices that will be chunked out when processing. Only relevant
  *                     for functions that use this class's distributedOperation function
  */
abstract class DistributedMatrix(private val rows:Int, private val cols:Int, private var defNumProcessors:Int, private var defBlocksize:Int) extends Matrix(rows, cols) {

  /** blocksize getter/setter */
  def getBlocksize = defBlocksize
  def setBlockSize(bs:Int):Unit = defBlocksize = bs

  /** num processors getter/setter */
  def getNumProcessors:Int = defNumProcessors
  def setNumProcessors(np:Int):Unit = defNumProcessors = np

  /** return the number of row "chunks" we have */
  def rowBlocks(blocksize:Int = getBlocksize) = Math.ceil(1.0*getRows/blocksize).toInt

  /** return the number of column "chunks" we have */
  def colBlocks(blocksize:Int = getBlocksize) = Math.ceil(1.0*getCols/blocksize).toInt

  /** total number of blocks */
  def numBlocks(blocksize:Int = getBlocksize) = rowBlocks(blocksize) * colBlocks(blocksize)

  /** Abstract distributed implementatons of basic matrix operations */
  def +(a: Double,numProcessors:Int ,blocksize:Int ):DistributedMatrix
  def +(m: Matrix,numProcessors:Int ,blocksize:Int ):DistributedMatrix
  def *(a: Double,numProcessors:Int ,blocksize:Int ):DistributedMatrix
  def *(m: Matrix,numProcessors:Int ,blocksize:Int ):DistributedMatrix
  def transpose(numProcessors:Int ,blocksize:Int ):DistributedMatrix
  def inverse(numProcessors:Int ,blocksize:Int ):DistributedMatrix

  /** We are going to implement the basic matrix operations from the Matrix superclass so we can introduce
    * distributed computing to them */
  def +(a: Double) = this.+(a,getNumProcessors,getBlocksize)
  def +(m: Matrix) = this.+(m,getNumProcessors,getBlocksize)
  def *(a: Double) = this.*(a,getNumProcessors,getBlocksize)
  def *(m: Matrix) = this.*(m,getNumProcessors,getBlocksize)
  def transpose = this.transpose(getNumProcessors,getBlocksize)
  def inverse = this.inverse(getNumProcessors,getBlocksize)

  /**
    * return the indices of the bound of each block of our matrix
    *
    * @param blocksize size of each block
    * @return
    */
  def getIndices(blocksize:Int = getBlocksize) = {

    // Array will be a 4-tuple: (block left bound, block right bound, block upper bound, block lower bound)
    val out = Array.ofDim[Tuple4[Int, Int, Int, Int]](numBlocks(blocksize))

    // For each block, add teh bounds to our array
    for(i <- 0 until rowBlocks(blocksize)){
      for(j <- 0 until colBlocks(blocksize)){
        out(i*colBlocks(blocksize)+j) = (i*blocksize,Math.min((i+1)*blocksize,getRows),j*blocksize,Math.min((j+1)*blocksize,getCols))
      }
    }
    out
  }

  /**
    * Perform a given distributed operation. This will make numProcessor number of threads that will keep computing blocks
    * until all blocks have been computed.
    *
    * @param func a function that maps any given cell to a particular action.
    * @param numProcessors number of processors to parallelize operations over
    * @param blocksize size of each block
    */
  def distributedOperation(func:Function2[Int,Int,Unit],numProcessors:Int = getNumProcessors,blocksize:Int = getBlocksize): Unit ={

    // what are the indices of the blocks?
    val indices = getIndices(blocksize)

    // volatile and Atomicinteger makes threaded access to this variable safe. We will keep computing blocks until
    // this variable indicates none are left to be computed
    @volatile var blocksLeft:AtomicInteger = new AtomicInteger(numBlocks(blocksize))

    // We will make an array of threads
    val threadids = (0 until numProcessors).toArray
    val threads = threadids.map{ id =>
      new Thread {
        override def run(): Unit ={

          // which block are we computing?
          var block = blocksLeft.getAndDecrement()
          while(block > 0){

            // for all cells within the given block, perform the given function on that cell
            val bounds = indices(block-1)
            for(i <- bounds._1 until bounds._2){
              for(j <- bounds._3 until bounds._4){
                func(i,j)
              }
            }

            // get the next block
            block = blocksLeft.getAndDecrement()
          }
        }
      }
    }

    // Start and join threads. Join is to make sure we don't continue on with the program until all threads are done
    threads.foreach(_.start)
    threads.foreach(_.join)
  }



}
