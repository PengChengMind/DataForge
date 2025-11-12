package org.apache.test

import scala.collection.mutable

object ScalaTest {

  val usage =
    """
    Usage: spark-submit --master=yarn xx.jar [-h] [-p] <inputCsvPath> [-o] <outputLogPath> [-f] <testFuncName>
  """

  type OptionMap = Map[Symbol, String]



  def distanceCal(leftArray: Array[Long], rightArray: Array[Long]): Double ={
    val interSize = leftArray.intersect(rightArray).size
    val unionSize = leftArray.size + rightArray.size - interSize
    val result = 1 - interSize * 1.0 / unionSize
    //    println("leftArray: " + leftArray.mkString(",") + "  rightArray: " + rightArray.mkString(","))
    //    println("interSize: " + interSize + " unionSize: " + unionSize + " result: " + result)
    result
  }

  def distanceCal2(leftArray: Array[Long], rightArray: Array[Long]): Double ={
    val leftSet = new mutable.HashSet[Long]
    var interSize = 0
    for (ele <- leftArray){
      leftSet.+=(ele)
    }
    for(ele <- rightArray){
      if(leftSet.contains(ele)){
        interSize += 1
      }
    }
    val unionSize = leftArray.size + rightArray.size - interSize
    val result = 1 - interSize * 1.0 / unionSize
    //    println("leftArray: " + leftArray.mkString(",") + "  rightArray: " + rightArray.mkString(","))
    //    println("interSize: " + interSize + " unionSize: " + unionSize + " result: " + result)
    result
  }

  def main(args: Array[String]): Unit = {

    val leftArray = Array[Long](1, 2, 34234, 3245234523l, 342523452435l, 2341233241412343241l, 345235434254325l,44444444444444l,66666666l, 235423452345l, 435345235425l, 43522346234623l, 34256234523452l, 3463454643674l, 543766534563456l, 43576345734567345l, 34567674l, 4536345634563456l, 34565467654746l, 4536457667l, 45222222222l, 4353534267899l, 999999999999l)
    val rightArray = Array[Long](1, 2, 435645555l, 5665657l, 6767676767l, 235423452345l, 435345235425l, 43522346234623l, 34256234523452l, 3463454643674l, 543766534563456l, 43576345734567345l, 34567674l, 4536345634563456l, 34565467654746l, 4536457667l, 45222222222l, 4353534267899l, 999999999999l)


    var start = System.currentTimeMillis()
    for (i <- 0 until 100000000){
      distanceCal(leftArray, rightArray)
    }
    var end = System.currentTimeMillis()
    println("time cost1 = " + (end - start) / 1000.0)

    start = System.currentTimeMillis()
    for (i <- 0 until 100000000){
      distanceCal2(leftArray, rightArray)
    }
    end = System.currentTimeMillis()
    println("time cost2 = " + (end - start) / 1000.0)
  }

}
