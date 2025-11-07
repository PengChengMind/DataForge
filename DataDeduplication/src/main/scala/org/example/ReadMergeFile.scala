package org.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.sys.process._

object ReadMergeFile {

  //定义主方法
  def main(args: Array[String]): Unit = {
    val separator = "%%%pcl%%%"
    var input = "hdfs://localhost:9000/user/stardust/input/mergeDir"
    var output = "output/mergeDirHash"
    //本地模式
    var conf = new SparkConf().setAppName("ReadMergeFile").setMaster("local")
    if(args.length == 2){
      input = args(0)
      output = args(1)
      conf = new SparkConf().setAppName("My Scala Word Count")
    } else {
      val cmd = "rm -rf " + output
      cmd.!!
    }
    print("version 1.0 input = " + input + " output = " + output)
    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val result = sc.wholeTextFiles(input) //读取数据
      .flatMap(x => x._2.split(separator))  //运算步骤
      .map(x => {val spArray = x.split("\n", 2); (spArray.apply(0), spArray.apply(1))})
      .zipWithIndex()
    result.foreach(println)
    result.saveAsTextFile(output) //保存数据
    sc.stop()
  }
}
