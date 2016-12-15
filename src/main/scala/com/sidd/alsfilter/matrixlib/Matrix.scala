package com.sidd.alsfilter.matrixlib

/**
  * Abstract matrix class that has all the basic operations that Matrix classes shoud have. All entries should be
  * initialized to 0.0
  *
  * @param rows number of rows in the matrix
  * @param cols number of columns in the matrix
  */
abstract class Matrix(private val rows:Int, private val cols:Int) {

  /** Getters and setters */
  def apply(row:Int, col:Int):Double
  def set(row:Int, col:Int, value:Double)
  def get(row:Int, col:Int) = apply(row,col)
  def getRows:Int = rows
  def getCols:Int = cols

  /** Basic matrix operations */
  def +(a:Double):Matrix
  def +(m:Matrix):Matrix
  def *(a:Double):Matrix
  def *(a:Matrix):Matrix
  def inverse:Matrix
  def transpose:Matrix

  /**
    * Require that this matrix have the same dimension as the given one. Useful for operations like addition.
    *
    * @param m the other matrix
    */
  def requireSameDim(m:Matrix):Unit = {

    if(m.getCols != this.getCols || m.getRows != this.getRows) {
      throw new Exception(s"Cannot perform addition  on $m and $this")
    }
  }

  /**
    * Require that this matrix has as many columns as the given matrix has rows. Useful for operations like multiplication.
    *
    * @param m the other matrix
    */
  def requireMult(m:Matrix):Unit = {

    if(this.getCols != m.getRows) {
      throw new Exception(s"Cannot perform multiplication on $m and $this")
    }
  }

  /**
    * Display the matrix, or part of it. Should not be used on big matrices.
    *
    * @param r optionally, the number of rows to display
    * @param c optionally, the number of columns to display
    */
  def display(r:Int=getRows, c:Int=getCols): Unit ={

    // Make sure r and c are less than the actual matrix size
    val nr = Math.min(r,rows)
    var nc = Math.min(c,cols)
    println("[")
    for(i <- 0 until nr){
      for(j <- 0 until nc){
        print(s"${get(i,j)}\t")
      }
      println()
    }
    println("]")

  }

  override def toString = s"Matrix: [${rows}x${cols}]"

}
