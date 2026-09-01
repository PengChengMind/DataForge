package org.example

import com.huaban.analysis.jieba.JiebaSegmenter
import component.TextStatistics
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import algorithm.simhash.{JiebaWordSeg, Simhash, SimhashFactory}
import org.utils.Tools
import tokenizer.JiebaTokenizer

import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.sys.process._
import scala.util.control.Breaks


object testPrefix {

  def main(args: Array[String]): Unit = {


    val timeBegin = System.currentTimeMillis()

    var distance = 0.5
    var haming_distance = 3
    var filterLength = 10

    var numPartitions = 1
    var groupPartitions = 4
    var langMode = "all"
    var debugMode = "work"

    var stopWordsPath = "cn_stopwords.txt"
    var input = "input/00001.txt"
    var output = "output"
    var appName = "SimJoinSimhashOrderIdf_" + "_" + distance.formatted("%.1f") + "_" + haming_distance + "_" + langMode + "_" + input.replaceAll("txt","").substring(Math.max(0, input.replaceAll("txt","").length - 15))
    var conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .setAppName(appName)
      .setMaster("local")
      .set("spark.sql.shuffle.partitions", "4")
    //      .setMaster("spark://stardustdeMacBook-Pro.local:7077")
    //      .set("spark.driver.memory", "512m")
    //      .set("spark.driver.cores", "1")
    //      .set("spark.executor.cores", "1")
    //      .set("spark.executor.memory", "512m")
    if (args.length == 10) {
      var pos = 0
      input = args(pos)
      pos = pos + 1
      output = args(pos)
      pos = pos + 1
      filterLength = args(pos).toInt
      pos = pos + 1
      distance = args(pos).toFloat
      pos = pos + 1
      haming_distance = args(pos).toInt
      pos = pos + 1
      numPartitions = args(pos).toInt
      pos = pos + 1
      groupPartitions = args(pos).toInt
      pos = pos + 1
      langMode = args(pos).toLowerCase
      pos = pos + 1
      debugMode = args(pos)
      pos = pos + 1
      appName = "SimJoinSimhashOrderIdf_" + "_" + distance.formatted("%.1f") + "_" + haming_distance + "_" + langMode + "_" + input.replaceAll("txt","").substring(Math.max(0, input.replaceAll("txt","").length - 15))
      conf = new SparkConf()
        .registerKryoClasses(Array(classOf[JiebaSegmenter]))
        .setAppName(appName)
        .set("spark.sql.shuffle.partitions", args(pos))
      pos = pos + 1
    } else {
      val cmd = "rm -rf " + output
      cmd.!!
    }

    println(appName + " version 1.0 input = " + input + " output = " + output + " threshold_distance = " + distance + " haming_distance: " + haming_distance)
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val segmenter = new JiebaSegmenter
    val simHash = new Simhash(new JiebaWordSeg)
    /* step1
        原始数据初步规整，编号和过滤无效文档
     */
    if(!Tools.isHdfsPathExist(sc, output + "/index_output")){
      sc.textFile(input)
        .repartition(numPartitions)
        .zipWithIndex()
        .filter(x => x._1.length > filterLength)
        .saveAsTextFile(output + "/index_output")
      println(output + "/index_output path dose not exist, created new index_output")
    }else {
      println("directly read index_path from " + output + "/index_output")
    }

    /*
       step2读取规整后的文档构成输入
     */
    val lines_ids_pattern = """^\((.*),(\d+)\)$""".r
    val lines_ids = sc.textFile(output + "/index_output")
      .map(x => lines_ids_pattern.findAllMatchIn(x).map(x => (x.group(1), x.group(2).toLong)).next())

    val spark = SparkSession.builder.getOrCreate()

    /* step3
      数据清洗、去停止词、分词、词频统计、计算TFIDF
     */
    val tokenizer = new JiebaTokenizer
    val textStatistics = new TextStatistics(output, stopWordsPath, langMode, tokenizer, debugMode)
    var record_tfidf_rdd = textStatistics.getTfIdfRdd(lines_ids, sc)
    if(!Tools.isHdfsPathExist(sc, output + "/record_tfidf_rdd")) {
      println("reacreate record_tfidf_rdd")
      record_tfidf_rdd.saveAsObjectFile(output + "/record_tfidf_rdd")
    } else {
      println("reused record_tfidf_rdd")
    }

    record_tfidf_rdd = sc.objectFile(output + "/record_tfidf_rdd")
    println("record_tfidf_rdd.take(3).foreach(println)")
    record_tfidf_rdd.take(3).foreach(x => println(x._1 + " : " + x._2.mkString(",")))


    /*
      前缀剪枝
     */
    import spark.implicits._
    val flat_prefix_rdd = record_tfidf_rdd
      .flatMap(prefixFlatMap(_, distance, simHash, debugMode))

    val group_df = flat_prefix_rdd
      .groupByKey(groupPartitions)
      .map(x => GroupList(x._1, x._2.toList.size))
      .toDF()

    if(!Tools.isHdfsPathExist(sc, output + "/group_df_first")) {
      println("recreate flat_prefix_rdd")
      group_df.write.parquet(output + "/group_df_first")
    } else {
      println("reused flat_prefix_rdd")
      val group_df_backup = spark.read.parquet(output + "/group_df_first")
      val diff_df = group_df.alias("df1")
        .join(group_df_backup.alias("df2"), group_df("token") ===  group_df_backup("token"))
       // .where(group_df("size") =!=  group_df_backup("size"))
        .select($"df1.*", $"df2.size".alias("size_2"))
      println("diff_df.show()")
      diff_df.show()
      group_df.write.mode("overwrite").csv(output + "/group_df")
      group_df_backup.write.mode("overwrite").csv(output + "/group_df_backup")
      diff_df.write.mode("overwrite").csv(output + "/diff_df")
    }
  }

//  case class Prefix(id: Long, tokenLength: Long, prefixLen: Int, sumWeight: Double, prefixWeight: Double, totalWeight: Double, simCode: Long)
case class GroupList(token: Long, size: Int)
  /*
    simhash实现的内部相似度验证
   */
  def groupSimhashFlatMap(iterTuple: (Long, Iterable[(Long, Array[(Long, Int, Double)], Double)]), haming_distance: Int, debugMode: String): Iterator[(Long, Long)] = {
    if(debugMode.equalsIgnoreCase("debug")){
      println("groupSimhashFlatMap " + iterTuple._1)
    }
    val iter_list = iterTuple._2.toList
    if(debugMode.equalsIgnoreCase("debug")){
      println(iter_list.map(x => x._1).mkString(","))
    }
    val list = iter_list.map(x => (java.lang.Long.valueOf(x._1), x._2.map(x => java.lang.Long.valueOf(x._1)).toList.asJava)).asJava
    val simList = SimhashFactory.getSimPairs(list, haming_distance, debugMode).asScala.map(x => (x._1.toLong, x._2.toLong))
    if(debugMode.equalsIgnoreCase("debug") && simList.length > 0) {
      println("simList: " + simList.mkString(","))
    }
    simList.iterator
  }

  /*
    simhash带权重
   */
  def groupSimhashFlatMapWithWeightFromList(iterTuple: (Long, Iterable[(Long, Array[(Long, Int, Double)], Double)]), haming_distance: Int, matchMode: String, debugMode: String): Iterator[(Long, Long)] = {

    if(debugMode.equalsIgnoreCase("debug")){
      println("groupSimhashFlatMap " + iterTuple._1)
    }
    val iter_list = iterTuple._2.toList
    if(debugMode.equalsIgnoreCase("debug")){
      println(iter_list.map(x => x._1).mkString(","))
    }

    val list = iter_list.map(x => (java.lang.Long.valueOf(x._1), x._2.map(x => (java.lang.Long.valueOf(x._1), java.lang.Integer.valueOf(x._2), java.lang.Double.valueOf(x._3))).toList.asJava)).asJava
    val simList = SimhashFactory.getSimWeightPairsFromList(list, haming_distance, matchMode, debugMode).asScala.map(x => (x._1.toLong, x._2.toLong))
    if(debugMode.equalsIgnoreCase("debug") && simList.length > 0) {
      println("simList: " + simList.mkString(","))
    }
    simList.iterator
  }

  def groupSimhashFlatMapWithWeight(iterTuple: (Long, Iterable[(Long, Long)]), haming_distance: Int, matchMode: String, debugMode: String): Iterator[(Long, Long)] = {

    if(debugMode.equalsIgnoreCase("debug")){
      println("groupSimhashFlatMap " + iterTuple._1)
    }
    val iter_list = iterTuple._2.toList
    if(debugMode.equalsIgnoreCase("debug")){
      println(iter_list.map(x => x._1).mkString(","))
    }

    val list = iter_list.map(x => (java.lang.Long.valueOf(x._1), java.lang.Long.valueOf(x._2))).asJava
    val simList = SimhashFactory.getSimWeightPairs(list, haming_distance, matchMode, debugMode).asScala.map(x => (x._1.toLong, x._2.toLong))
    if(debugMode.equalsIgnoreCase("debug") && simList.length > 0) {
      println("simList: " + simList.mkString(","))
    }
    simList.iterator
  }



  /*
    计算绝对前缀并分发数据
   */
  def prefixFlatMap(lines: (Long, Array[(Long, Int, Double)]), threshold: Double, simHash: Simhash, debugMode: String): Iterator[(Long, (Long, Long))] = {
    val id = lines._1
    val idf_tokens = lines._2
    var total_weight = 0.0
    for (x <- lines._2){
      total_weight += x._3
    }

    val list = idf_tokens.map(x => (java.lang.Long.valueOf(x._1), java.lang.Integer.valueOf(x._2), java.lang.Double.valueOf(x._3))).toList.asJava
    val simCode = simHash.simhash64WithWeight(list)

    val prefix_weight = total_weight * threshold
    var sum_weight = 0.0
    val loop = new Breaks
    var index = 0
    loop.breakable {
      while (index < idf_tokens.length) {
        sum_weight += idf_tokens.apply(index)._3
        if (sum_weight > prefix_weight) {
          loop.break()
        }
        index = index + 1
      }
    }
    if(debugMode.equalsIgnoreCase("debug")){
      println("id=" + id + " tokenLen=" + idf_tokens.length + " prefixLen=" + (index+1) + " sum_weight=" + sum_weight + " prefix_weight=" + prefix_weight + " total_weight=" + total_weight)
    }
    //    println("id=" + id + " tokenLen=" + idf_tokens.length + " prefixLen=" + (index+1) + " sum_weight=" + sum_weight + " prefix_weight=" + prefix_weight + " total_weight=" + total_weight)
    //    println("idf_tokens " + idf_tokens.mkString(","))
    val arrayBuffer = new ArrayBuffer[(Long, (Long, Long))]
    for(i <- 0 to Math.min(index, idf_tokens.length - 1)){
      arrayBuffer.append((idf_tokens.apply(i)._1, (id, simCode)))
    }
    arrayBuffer.iterator
  }




}
