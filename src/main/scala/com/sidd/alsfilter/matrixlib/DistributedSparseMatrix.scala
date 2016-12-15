package com.sidd.alsfilter.matrixlib

import collection.mutable.Map

/**
  * Concrete sparse and distributed matrix. There will be a stored value for each non-zero cell in the matrix.
  * Because this library was designed specifically for Alternating Least Squares, many functions have not been
  * implemented because they will not be used.
  *
  * @param rows number of rows in the matrix
  * @param cols number of columns in the matrix
  * @param numProcessors number of processors
  * @param blocksize size of each block
  */
class DistributedSparseMatrix (private val rows:Int,
                               private val cols:Int,
                               private var numProcessors:Int=1,
                               private val blocksize:Int = 1000)
  extends DistributedMatrix(rows, cols,numProcessors,blocksize) {

  /** We keep a row map and a column map because one or the other will be helpful in multiplication depending on
   * which matrix is first */
  val rowMap:Map[Int,Map[Int,Double]] = Map[Int,Map[Int,Double]]()
  val colMap:Map[Int,Map[Int,Double]] = Map[Int,Map[Int,Double]]()

  /**
    * Not implemented
    *
    * @param a
    * @param numProcessors
    * @param blocksize
    * @return
    */
  override def +(a: Double, numProcessors: Int, blocksize: Int): DistributedMatrix = {
    throw new UnsupportedOperationException("Does not need to be implemented for ALS")
  }

  /**
    * Not implemented
    *
    * @param numProcessors
    * @param blocksize
    * @return
    */
  override def inverse(numProcessors: Int, blocksize: Int): DistributedMatrix = {
    throw new UnsupportedOperationException("Does not need to be implemented for ALS")
  }

  /**
    * return the transpose of this matrix
    *
    * @param numProcessors
    * @param blocksize
    * @return the transpose of the matrix
    */
  override def transpose(numProcessors:Int = getNumProcessors,blocksize:Int = getBlocksize) = {

    val out = new DistributedSparseMatrix(cols,rows,numProcessors,blocksize)
    for(row <- rowMap.keys){
      for(col <- rowMap.get(row).get.keys){
        out.set(col,row,rowMap.get(row).get.get(col).get)
      }
    }
    out
  }

  /**
    * Not implemented
    *
    * @param m
    * @param numProcessors
    * @param blocksize
    * @return
    */
  override def +(m: Matrix, numProcessors: Int, blocksize: Int): DistributedMatrix = {
    throw new UnsupportedOperationException("Does not need to be implemented for ALS")
  }

  /**
    * Not implemented
    *
    * @param a
    * @param numProcessors
    * @param blocksize
    * @return
    */
  override def *(a: Double, numProcessors: Int, blocksize: Int): DistributedMatrix = {
    throw new UnsupportedOperationException("Does not need to be implemented for ALS")
  }

  /**
    * Compute the product of this matrix with another matrix
    *
    * @param m the matrix to multiply with
    * @param numProcessors
    * @param blocksize
    * @return the product of the matrices
    */
  override def *(m: Matrix, numProcessors: Int, blocksize: Int): DistributedMatrix = {

    // Matrices must be multipliable
    requireMult(m)

    val outMatrix = new DistributedDenseMatrix(rows,m.getCols,numProcessors,blocksize)

    outMatrix.distributedOperation((i,j)=>{
      var sum = 0.0

      // We can do this efficiently by only considering non-zero values in this matrix
      for(col <- rowMap.getOrElse(i,Map[Int,Double]()).keys){
        sum+=this(i,col)*m(col,j)
      }
      outMatrix.set(i,j,sum)
    })
    outMatrix

  }

  /**
    * Set values in this matrix
    *
    * @param row the cell's row
    * @param col the cell's column
    * @param value the cell's new value
    */
  override def set(row: Int, col: Int, value: Double): Unit = {

    // Make sure we are in bounds
    if(row>=getRows || col >= getCols){
      throw new IndexOutOfBoundsException(s"($row,$col) is out of bounds in $this.")
    }

    // Update rowMap
    if(!rowMap.contains(row)) {
      rowMap.put(row,Map[Int,Double]())
    }
    rowMap.get(row).get.put(col,value)

    // if the value is 0.0, then remove it from rowMap
    if(value==0.0) rowMap.get(row).get.remove(col)
    if(rowMap.get(row).get.size==0) rowMap.remove(row)

    // Update colMap
    if(!colMap.contains(col)) {
      colMap.put(col,Map[Int,Double]())
    }
    colMap.get(col).get.put(row,value)

    // if the value is 0.0, then remove it from colMap
    if(value==0.0) colMap.get(col).get.remove(row)
    if(colMap.get(col).get.size==0) colMap.remove(col)
  }

  /** getter */
  override def apply(row: Int, col: Int): Double = {
    rowMap.getOrElse(row,Map[Int,Double]()).getOrElse(col,0.0)
  }
}
