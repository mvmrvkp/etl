package com.venkata.mohan.spark.etl.udf

object TestUdf extends Serializable{

  /*def incrementSalUdf = udf((sal: Double, incPer: Double)=> {
    incrementSal(sal, incPer)
  })*/

  def incrementSalUdf(sal: Double, incPer: Double): Double ={
    return sal * incPer
  }

  /*def incrementSalUdf = udf((sal: Double, incPer: Double)=> {
    sal * incPer
  })*/

}
