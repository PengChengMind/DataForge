package org.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {

  //定义主方法
  def main(args: Array[String]): Unit = {

    var input = "input/air_train_data_1000.csv"
    var output = "output/scala_wordcount_result"
    //本地模式
    var conf = new SparkConf().setAppName("My Scala Word Count").setMaster("local")
    if(args.length == 2){
      input = args(0)
      output = args(1)
      conf = new SparkConf().setAppName("My Scala Word Count")
    }
    print("version 1.0 input = " + input + " output = " + output)
    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val result = sc.textFile(input) //读取数据
      .flatMap(_.split(" "))  //运算步骤
      .map((_,1))
      .reduceByKey(_+_)

    result.foreach(println)
    result.saveAsTextFile(output) //保存数据
    sc.stop()


  }
}
