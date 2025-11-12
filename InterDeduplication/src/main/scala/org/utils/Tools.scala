package org.utils

import com.alibaba.fastjson.JSONObject
import com.google.gson.JsonObject
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object Tools {

  val logger = LoggerFactory.getLogger(this.getClass)


  def distanceCal(leftArray: Array[Int], rightArray: Array[Int]): Double ={
    val leftSet = new mutable.HashSet[Int]
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

  def distanceCal(leftArray: Array[Long], rightArray: Array[Long]): Double ={
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


  def distanceCal(leftArray: Array[(Long, Double)], rightArray: Array[(Long, Double)], leftTotal: Double, rightTotal: Double): Double ={

    var leftSet = new HashSet[Long]()
    for (ele <- leftArray){
      leftSet.+=(ele._1)
    }
    var interWeight = 0.0
    for (ele <- rightArray){
      if(leftSet.contains(ele._1)){
        interWeight += ele._2
      }
    }
    val result = 1 - interWeight / (leftTotal + rightTotal - interWeight)
//    println("leftArray: " + leftArray.mkString(",") + "  rightArray: " + rightArray.mkString(","))
//    println("interWeight: " + interWeight + " unionWeight: " + (leftTotal + rightTotal - interWeight) + " result: " + result)
    result
  }

  def distanceCalInt(leftArray: Array[(Int, Double)], rightArray: Array[(Int, Double)], leftTotal: Double, rightTotal: Double): Double ={

    var leftSet = new HashSet[Long]()
    for (ele <- leftArray){
      leftSet.+=(ele._1)
    }
    var interWeight = 0.0
    for (ele <- rightArray){
      if(leftSet.contains(ele._1)){
        interWeight += ele._2
      }
    }
    val result = 1 - interWeight / (leftTotal + rightTotal - interWeight)
    //    println("leftArray: " + leftArray.mkString(",") + "  rightArray: " + rightArray.mkString(","))
    //    println("interWeight: " + interWeight + " unionWeight: " + (leftTotal + rightTotal - interWeight) + " result: " + result)
    result
  }

  def deleteHdfsPath(sc: SparkContext, filePath: String): Unit = {
    logger.info("deleteHdfsPath: " + filePath)
    val path = new Path(filePath)
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if(hdfs.exists(path)){
      hdfs.delete(path, true)
    }
  }

  def saveRdd[T](sc: SparkContext, rdd: RDD[T], filePath: String): Unit = {
    deleteHdfsPath(sc, filePath)
    rdd.saveAsTextFile(filePath) //保存数据
  }


  def isContainText(str: String): Boolean = {

    val regex = new Regex("[\\u4e00-\\u9fa5_a-zA-Z0-9]")
    regex.findFirstMatchIn(str) != None
  }


  def isHdfsPathExist(sc: SparkContext, filePath: String): Boolean = {
    logger.info("isHdfsPathExist filePath: " + filePath)
    val path = new Path(filePath)
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    hdfs.exists(path)
  }

  def createPath(sc: SparkContext, filePath: String): Unit = {
    if(!isHdfsPathExist(sc, filePath)){
      logger.info("createPath filePath: " + filePath)
      val path = new Path(filePath)
      val hadoopConf = sc.hadoopConfiguration
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      hdfs.mkdirs(path)
    }
  }



  def printPartitionNum[T](description: String, rdd: RDD[T]): Array[(Int,Int)] = {

    val joinPartNum = rdd.mapPartitionsWithIndex {
      (partIdx, iter) => {
        var count = 0
        while (iter.hasNext) {
          count = count + 1
          iter.next()
        }
        Array((partIdx, count)).iterator
      }
    }.collect
    println(description + ": total=" + joinPartNum.map(x => x._2).sum + " ; " + joinPartNum.sortWith(_._2 > _._2).map( x => (x._1 + ":" + x._2)).mkString(","))
    joinPartNum
  }


  def printGroupPartitionNum[T,V](description: String, rdd: RDD[(T, Iterable[V])]): Array[(Int,Int, Array[(T, Int)])] = {

    val joinPartNum = rdd.mapPartitionsWithIndex {
      (partIdx, iter) => {
        var count = 0
        var countBuffer = new ArrayBuffer[(T, Int)]()
        while (iter.hasNext) {
          val element = iter.next()
          var inner_count = 0
          val inner_iter = element._2.iterator
          while(inner_iter.hasNext){
            count = count + 1
            inner_count = inner_count + 1
//            println("inner " + inner_count)
            inner_iter.next()
          }
//          println("outer " + count)
          countBuffer.append((element._1, inner_count))
        }
        Array((partIdx, count, countBuffer.toArray)).iterator
      }
    }.collect
    println("description count=" +  rdd.count())
    println(description + ": total=" + joinPartNum.map(x => x._2).sum + " ; " + joinPartNum.sortWith(_._2 > _._2).map( x => (x._1 + ":" + x._2)).mkString(","))
    joinPartNum
  }

  def getJsonValue(jsonObject: JSONObject, key: String, defaultValue: Int): Int = {
    val value = jsonObject.getIntValue(key)
    if(value == 0){
      defaultValue
    } else {
      value
    }
  }

  def getJsonString(jsonObject: JSONObject, key: String, defaultValue: Int): Int = {
    val value = jsonObject.getIntValue(key)
    if(value == 0){
      defaultValue
    } else {
      value
    }
  }

}
