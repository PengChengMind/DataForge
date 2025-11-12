package org.example

import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.immutable.HashSet
import scala.io.Source
import scala.sys.process._

object WordCountWithJieba {

  //定义主方法
  def main(args: Array[String]): Unit = {

    var input = "input/00001.txt"
    var output = "output/scala_wordcount_result"

    val segmenter = new JiebaSegmenter
    //本地模式
    var conf = new SparkConf().setAppName("My Scala Word Count").setMaster("local")
    if(args.length == 2){
      input = args(0)
      output = args(1)
      conf = new SparkConf().setAppName("My Scala Word Count")
    }
    val cmd = "rm -rf " + output
    cmd.!!
    print("version 1.0 input = " + input + " output = " + output)
    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val pattern = """[^\u4e00-\u9fa5]""".r
    var stopSet = HashSet[String]()
    val stopWordsFile = Source.fromResource("cn_stopwords.txt")
    stopWordsFile.getLines().foreach(stopSet += _)
    stopSet.take(100).foreach(println)

    val bad_result = sc.textFile(input) // 保留停止词不清洗
      .filter(x => x.length > 3)
      .flatMap(x => segmenter.sentenceProcess(x).toArray)
      .map(x => x.toString())
      .map((_,1))
      .reduceByKey(_+_)
      .sortBy(_._2,false)

    val good_result = sc.textFile(input) //消除停止词并清洗
      .filter(x => x.length > 3)
      .flatMap(x => segmenter.sentenceProcess(pattern.replaceAllIn(x,"")).toArray)
      .map(x => x.toString())
      .filter(!stopSet.contains(_))
      .map((_,1))
      .reduceByKey(_+_)
      .sortBy(_._2,false)

    bad_result.take(100).foreach(println)
    good_result.take(100).foreach(println)
    bad_result.saveAsTextFile(output + "/bad_result") //保存数据
    good_result.saveAsTextFile(output + "/good_result") //保存数据


    sc.stop()
  }
}
