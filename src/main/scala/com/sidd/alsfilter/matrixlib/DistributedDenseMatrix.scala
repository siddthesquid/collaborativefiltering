package com.sidd.alsfilter.matrixlib

import java.util.concurrent.atomic.AtomicInteger

/**
  * Concrete dense and distributed matrix. There will be a stored value for each cell in the matrix.
  *
  * @param rows number of rows in the matrix
  * @param cols number of columns in the matrix
  * @param numProcessors number of processors to parallelize operations over
  * @param blocksize size of each block
  */
class DistributedDenseMatrix(private val rows:Int,
                             private val cols:Int,
                             private var numProcessors:Int=1,
                             private val blocksize:Int = 1000)
  extends DistributedMatrix(rows, cols,numProcessors,blocksize) {

  /** Entries are stored in a two dimensional array */
  val matrix = Array.ofDim[Double](rows,cols)

  /** Matrix getter/setter */
  override def apply(row: Int, col: Int): Double = matrix(row)(col)
  override def set(row: Int, col: Int, value: Double): Unit = {
    matrix(row)(col) = value
  }

  /**
    * return the transpose of this matrix
    *
    * @param numProcessors
    * @param blocksize
    * @return the transpose of the matrix
    */
  override def transpose(numProcessors:Int = getNumProcessors,blocksize:Int = getBlocksize) = {

    val outMatrix = new DistributedDenseMatrix(cols,rows,numProcessors,blocksize)
    outMatrix.distributedOperation((i,j)=>outMatrix.set(i,j,matrix(j)(i)))
    outMatrix
  }

  /**
    * add a double to every entry in our matrix
    *
    * @param a value to add to each cell
    * @param numProcessors
    * @param blocksize
    * @return matrix with the value added to each element
    */
  override def +(a: Double,numProcessors:Int,blocksize:Int ): DistributedMatrix = {

    val outMatrix = new DistributedDenseMatrix(rows,cols,numProcessors,blocksize)
    outMatrix.distributedOperation((i,j)=>outMatrix.set(i,j,matrix(i)(j)+a))
    outMatrix
  }

  /**
    * add another matrix to this matrix
    *
    * @param m another matrix
    * @param numProcessors
    * @param blocksize
    * @return sum of the two matrices
    */
  override def +(m: Matrix,numProcessors:Int = getNumProcessors,blocksize:Int = getBlocksize): DistributedMatrix = {

    // Matrices must be same dimensions
    requireSameDim(m)

    val outMatrix = new DistributedDenseMatrix(rows,cols,numProcessors,blocksize)
    outMatrix.distributedOperation((i,j)=>outMatrix.set(i,j,matrix(i)(j)+m.get(i,j)))
    outMatrix
  }

  /**
    * multiply a double to every entry in our matrix
    *
    * @param a value to multiply to each cell
    * @param numProcessors
    * @param blocksize
    * @return matrix with the value multipled to each element
    */
  override def *(a: Double,numProcessors:Int,blocksize:Int): DistributedMatrix = {

    val outMatrix = new DistributedDenseMatrix(rows,cols,numProcessors,blocksize)
    outMatrix.distributedOperation((i,j)=>outMatrix.set(i,j,matrix(i)(j)*a))
    outMatrix
  }

  /**
    * multiply two matrices together
    *
    * @param m another matrix
    * @param numProcessors
    * @param blocksize
    * @return product of two matrices
    */
  override def *(m: Matrix,numProcessors:Int = getNumProcessors,blocksize:Int = getBlocksize): DistributedMatrix = {

    // Matrices must be multipliable
    requireMult(m)

    val outMatrix = new DistributedDenseMatrix(rows,m.getCols,numProcessors,blocksize)

    // If the matrix we are multiplying is a sparse matrix, then many multiplications we perform will be 0, and we
    // can easily skip those
    if(m.isInstanceOf[DistributedSparseMatrix]){
      outMatrix.distributedOperation((i,j) => {
        var sum = 0.0

        // we retrieve the colMap because we are multiplying columns of the sparse matrix
        val colMap = m.asInstanceOf[DistributedSparseMatrix].colMap
        for(row <- colMap.getOrElse(j,Map[Int,Double]()).keys){
          sum+=this(i,row)*m(row,j)
        }
        outMatrix.set(i,j,sum)
      })

      // Otherwise, we are multiply dense matrices and need to go through every value
    } else {
      outMatrix.distributedOperation((i, j) => {
        var sum = 0.0
        for (k <- 0 until this.getCols) {
          sum += this (i, k) * m(k, j)
        }
        outMatrix.set(i, j, sum)
      })
    }

    outMatrix
  }

  /**
    * Find the inverse of the matrix. This will only find the inverse of positive definite symmetric matrices.
    *
    * @param numProcessors
    * @param blocksize
    * @return the inverse of the matrix
    */
  override def inverse(numProcessors:Int = getNumProcessors,blocksize:Int = getBlocksize): DistributedMatrix = {

    // Make sure we have a square matrix.
    if (getRows != getCols) {
      throw new Exception("Cannot invert a non-square matrix.")
    }

    // First find L such that M = LL^T using Cholesky Decomposition, and then find L^-1 using our special
    // lower triangular inverse function
    val lowerInvM = choleskyLower(numProcessors,blocksize).lowerInverse(numProcessors)

    // A^-1 = (L^-1)^T(L^-1)
    lowerInvM.transpose(numProcessors,blocksize) * lowerInvM

  }

  /**
    * Find L for some square matrix M such that M = LL^T
    *
    * @param numProcessors
    * @param blocksize
    * @return The Cholesky decomposed lower triangular matrix
    */
  def choleskyLower(numProcessors:Int = getNumProcessors,blocksize:Int = getBlocksize): DistributedDenseMatrix = {

    // Make sure we have a square matrix
    if (getRows != getCols) {
      throw new Exception("Cannot decompose a non-square matrix.")
    }

    val lower = new DistributedDenseMatrix(getRows,getRows,numProcessors,blocksize)

    // Each phase consists of a diagonal of the matrix. We must compute the diagonals sequentially
    val totalPhases = getRows * 2 - 1
    for (phase <- 0 until totalPhases) {

      // However, each diagonal computation can be parallelized. We do that here in each phase
      @volatile var cellsLeft: AtomicInteger = new AtomicInteger((Math.min(phase + 1, totalPhases - phase) + 1) / 2)

      val threadids = (0 until numProcessors).toArray
      val threads = threadids.map { id =>
        new Thread {
          override def run(): Unit = {

            // Which cell are we currently computing?
            var relCell = cellsLeft.getAndDecrement()
            while (relCell > 0) {
              val offset = relCell - 1
              val cell = (Math.min(phase, getRows - 1) - offset, Math.max(0, -1 * totalPhases / 2 + phase) + offset)

              // Outsourced computation of cell
              lower.set(cell._1,cell._2,calculateCholeskyCell(cell,lower))

              relCell = cellsLeft.getAndDecrement()
            }
          }
        }
      }

      // Start each thread and join them so we don't execute anything until all threads are complete
      threads.foreach(_.start)
      threads.foreach(_.join)
    }

    lower
  }

  /**
    * Helper function for Cholesky decomposition. Given a cell to compute and a lower triangular matrix, and
    * assuming all dependencies of that cell have been met, compute the value at that cell
    *
    * @param cell the cell to compute
    * @return
    */
  def calculateCholeskyCell(cell:(Int,Int),lower:DistributedDenseMatrix):Double = {

    val (i,k) = cell
    var sum = get(i,k)

    // If we are computing along the diagonal...
    if(i==k){
      for(j <- 0 until k){
        sum-=Math.pow(lower.get(k,j),2)
      }
      sum = Math.sqrt(sum)

      // If we are computing below the diagonal...
    } else {
      for(j <- 0 until k){
        sum -= lower.get(i,j) * lower.get(k,j)
      }
      sum = sum/lower.get(k,k)
    }

    sum
  }

  /**
    * Calculate the inverse of a lower triangular square matrix using forward substitution
    *
    * @param numProcessors
    * @return
    */
  def lowerInverse(numProcessors:Int = getNumProcessors): DistributedDenseMatrix = {

    // Make sure we have a square matrix
    if (getRows != getCols) {
      throw new Exception("Cannot invert a non-square matrix.")
    }

    val lowInv = new DistributedDenseMatrix(getRows,getCols,numProcessors,blocksize)

    // We will calculate the columns individually
    @volatile var colsLeft: AtomicInteger = new AtomicInteger(getCols)

    // Make an array of threads
    val threadids = (0 until numProcessors).toArray
    val threads = threadids.map { id =>
      new Thread {
        override def run(): Unit = {
          var col = colsLeft.getAndDecrement() - 1
          while (col >= 0) {

            // We will solve this column from top to bottom
            for(i <- 0 until getRows){
              var sum = if (col==i) 1.0 else 0.0

              // Aggregate what we have calculated so far in this column
              for(j <- 0 until i){
                sum-=(get(i,j)*lowInv.get(j,col))
              }

              val insert = sum/get(i,i)

              // Sometimes, we might do 0.0/0.0. Make sure NaN results in 0
              lowInv.set(i,col,if(insert.isNaN) 0 else insert)
            }
            col = colsLeft.getAndDecrement() - 1
          }
        }
      }
    }

    // Start each thread and join them so we don't execute anything until all threads are complete
    threads.foreach(_.start)
    threads.foreach(_.join)
    lowInv
  }

}

/**
  * Some static functions to quickly create a DistributedDenseMatrix
  */
object DistributedDenseMatrix {

  /**
    * Create an identity matrix
    *
    * @param n the size of the array
    * @param numProcessers
    * @param blocksize
    * @return an identity matrix of rank n
    */
  def eye(n:Int,numProcessers:Int=1,blocksize:Int=1000):DistributedDenseMatrix = {

    val newMatrix = new DistributedDenseMatrix(n,n,numProcessers,blocksize)
    for(i <- 0 until n){
      newMatrix.set(i,i,1.0)
    }
    newMatrix
  }

  /**
    * Create a matrix filled with random values
    *
    * @param r number of rows
    * @param c number of columns
    * @param n maximum value in each cell
    * @param numProcessers
    * @param blocksize
    * @return
    */
  def random(r:Int, c:Int, n:Double, numProcessers:Int=1,blocksize:Int=1000):DistributedDenseMatrix = {

    val newMatrix = new DistributedDenseMatrix(r,c,numProcessers,blocksize)
    newMatrix.distributedOperation((i,j)=>{newMatrix.set(i,j,Math.random()*n)})
    newMatrix
  }
}