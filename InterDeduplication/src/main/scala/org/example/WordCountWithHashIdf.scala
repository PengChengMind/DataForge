package org.example

import com.huaban.analysis.jieba.JiebaSegmenter
import component.TextStatistics
import org.apache.hadoop.util.hash.MurmurHash
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import tokenizer.JiebaTokenizer

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.io.Source
import scala.sys.process._
import scala.util.hashing.MurmurHash3
import scala.util.matching.Regex

object WordCountWithHashIdf {

  //定义主方法
  def main(args: Array[String]): Unit = {

    var input = "input/mini.txt"
    var output = "output/scala_wordcount_result"

    //本地模式
    var conf = new SparkConf().setAppName("My Scala Word Count").setMaster("local")
    if(args.length == 2){
      input = args(0)
      output = args(1)
      conf = new SparkConf().setAppName("My Scala Word Count")
    }
    val cmd = "rm -rf " + output
    cmd.!!
    println("version 1.0 input = " + input + " output = " + output)
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val recordRdd = sc.textFile(input)
    /*
     加上ID
     */
    val lines_ids = recordRdd.zipWithIndex().filter(x => x._1.length > 10).persist(StorageLevel.MEMORY_AND_DISK)
    //    println("lines_ids.take(3) ")
    //    lines_ids.take(3).foreach(println)
    lines_ids.saveAsTextFile(output + "/index_output")

    /*
      计算TF-IDF
     */
    val tokenizer = new JiebaTokenizer
    val textStatistics = new TextStatistics(rootPath = output, tokenizer = tokenizer, debugMode = "debug")
    val record_tfidf_rdd = textStatistics.getTfIdfRdd(lines_ids, sc)
    println("record_tfidf_rdd.take(10)")
    record_tfidf_rdd.take(10).foreach(x => println(x._1 + ": " + x._2.mkString(",")))
    record_tfidf_rdd.saveAsTextFile(output + "/record_tfidf")
    sc.stop()
  }




}
