package org.example

import com.huaban.analysis.jieba.JiebaSegmenter
import component.TextDistinct
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import algorithm.simhash.SimhashFactory
import org.utils.Tools

import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.sys.process._
import scala.util.control.Breaks



object SimJoinSimhashOrderIdf {

  def main(args: Array[String]): Unit = {


    val timeBegin = System.currentTimeMillis()

    var distance = 0.5
    var haming_distance = 3
    var filterLength = 10
    var num_hashFeature = 5000

    var numPartitions = 1
    var groupPartitions = 4
    var langMode = "all"
    var matchMode = "fuzzy"
    var debugMode = "work"

    var input = "input/mini.txt"
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
    if (args.length == 12) {
      var pos = 0
      input = args(pos)
      pos = pos + 1
      output = args(pos)
      pos = pos + 1
      filterLength = args(pos).toInt
      pos = pos + 1
      distance = args(pos).toFloat
      pos = pos + 1
      num_hashFeature = args(pos).toInt
      pos = pos + 1
      haming_distance = args(pos).toInt
      pos = pos + 1
      numPartitions = args(pos).toInt
      pos = pos + 1
      groupPartitions = args(pos).toInt
      pos = pos + 1
      langMode = args(pos).toLowerCase
      pos = pos + 1
      matchMode = args(pos).toLowerCase
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


    val spark = SparkSession.builder.getOrCreate()

    //读取文件，如果一个文件大小不超过hdfs 的一个 block，则一个文件的所有行
    // 都只在一个 partition 中，且一个 partition 只包含一个文件内容
    val lines = sc.textFile(input).repartition(numPartitions)
    // stage1 读文件分区，shuffle写


    //        println("lines: ")
    //        lines.take(3).foreach(println)
    // stage2 shuffle读、filter过滤、zipWithIndex编号、map分词、RDD转DataFrame、

    // 加上 ID
    val lines_ids = lines.zipWithIndex().filter(x => x._1.length > 10).persist(StorageLevel.MEMORY_AND_DISK)
    //    println("lines_ids.take(3) ")
    //    lines_ids.take(3).foreach(println)

    lines_ids.saveAsTextFile(output + "/index_output")

    val lines_id_schema: StructType = StructType(Array(
      StructField("id", LongType),
      StructField("words", StringType)
    ),
    )

    val origin_rdd_row = lines_ids.map(x => Row(x._2, x._1))

    val origin_df = spark.createDataFrame(origin_rdd_row, lines_id_schema)
    //    origin_df.show(10, 150)

    val pattern = if ("cn".equals(langMode)) {
      """[^\u4e00-\u9fa5]""".r
    } else if ("en".equals(langMode)) {
      """[^a-zA-Z]""".r
    } else {
      """[^\u4e00-\u9fa5_a-zA-Z0-9]""".r
    }
    var stopSet = HashSet[String]()
    val stopWordsFile = Source.fromResource("cn_stopwords.txt")
    stopWordsFile.getLines().foreach(stopSet += _)
    //    stopSet.take(10).foreach(println)

    lines_ids.map(x => (x._2, segmenter.sentenceProcess(x._1)))
    val ids_tokenss_split = lines_ids.map(x => (x._2, segmenter.sentenceProcess(pattern.replaceAllIn(x._1, "")).toArray))

    //    println("id_tokenss_split")
    //    ids_tokenss_split.take(3).foreach(x => println(x._1 + " : " + x._2.mkString(",")))

    val ids_tokenss = ids_tokenss_split.map(x => Row(x._1, x._2.map(_.toString).filter(!stopSet.contains(_))))

    //    println("ids_tokenss.take(3)")
    //    ids_tokenss.take(3).foreach(x => println(x.get(0) + " : " + x.getAs[Array[String]](1).mkString(",")))


    val ids_tokenss_schema: StructType = StructType(Array(
      StructField("id", LongType),
      StructField("words", ArrayType(StringType)
      )),
    )

    // RDD 转 DataFrame
    val ids_tokenss_df = spark.createDataFrame(ids_tokenss, ids_tokenss_schema)

    //计算 hashTF feature
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("features")
      .setNumFeatures(num_hashFeature)

    val ids_hashingTf_df = hashingTF.transform(ids_tokenss_df)
    println("ids_hashingTf_df.show(20, 150)")
    ids_hashingTf_df.show(20, 150)

    val idf = new IDF().setInputCol("features").setOutputCol("idf_features")
    val idfModel = idf.fit(ids_hashingTf_df)
    val rescaledData = idfModel.transform(ids_hashingTf_df)

    println("rescaledData.show(20, 150)")
    rescaledData.show(20, 150)

    val ids_hasing_save = ids_hashingTf_df.rdd.map(x => (x.getAs[Long]("id"), x.getAs[String]("words"), x.getAs[SparseVector]("features").indices.mkString(",")))

    //    ids_hashingTf_df.show(20, 150)
    //    ids_hasing_save.take(100).foreach(println)
    ids_hasing_save.saveAsTextFile(output + "/hashing_output")

    //    ids_hashingTf_df = ids_hashingTf_df.drop("words")

    //    println("ids_hashingTf_df.count() = " + ids_hashingTf_df.count())

    //    val ds = ids_hashingTf_df.as[(Long, String)]
    //    ds.flatMap {
    //      case (x1, x2, x3, x4) => x3.split(",").map((x1, x2, _, x4)):
    //    }.toDF

    /*
      词频统计
     */
    val word_count_rdd = ids_hashingTf_df.rdd.flatMap(_.getAs[SparseVector]("features").indices) //运算步骤
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
    //    println("words count")
    //    word_count_rdd.take(20).foreach(println)
    word_count_rdd.saveAsTextFile(output + "/wordcount_output")

    val wordscountMap = word_count_rdd.collectAsMap()
    val wordscountBroad = sc.broadcast(wordscountMap)

    val flat_perfix_rdd = rescaledData.rdd
      .flatMap(prefixFlatMap(_, distance, debugMode))
    //        flat_perfix_rdd.foreach(x => println("wordsPrefix= " + x._1 + " -> " +  x._2._1 + ",[" + x._2._2.mkString(",") + "]"))
    //        flat_perfix_rdd.take(20).foreach(x => println("token = " + x._1 + " -> record_id=" +  x._2._1));
    val group_rdd = flat_perfix_rdd
      .groupByKey(groupPartitions)

    val distinct_rdd = group_rdd
      .flatMap(groupSimhashFlatMapWithWeight(_, haming_distance, matchMode, debugMode))
      .distinct()

    //        tools.printPartitionNum("ids_hashingTf_df: ", ids_hashingTf_df.rdd)
    //        tools.printPartitionNum("flat_perfix_rdd: ", flat_perfix_rdd)
    if (debugMode.equalsIgnoreCase("debug")) {
      Tools.printGroupPartitionNum("group_rdd elements: ", group_rdd)
    }
//        tools.printPartitionNum("distinct_rdd elements: ", distinct_rdd)
    println("distinct_rdd.take(20).foreach(println)")
    distinct_rdd.take(20).foreach(println)
    val timeEnd = System.currentTimeMillis()

    println("time cost: " + (timeEnd - timeBegin))
    distinct_rdd.saveAsTextFile(output + "/sim_output")

    val textDistinct = new TextDistinct(output, debugMode)
    textDistinct.saveDeduplicateLineResult(distinct_rdd, lines_ids, spark, sc)
  }

  /*
    simhash实现的内部相似度验证
   */
  def groupSimhashFlatMap(iterTuple: (Int, Iterable[(Long, Array[(Int, Double)], Double)]), haming_distance: Int, debugMode: String): Iterator[(Long, Long)] = {
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
  def groupSimhashFlatMapWithWeight(iterTuple: (Int, Iterable[(Long, Array[(Int, String, Double)], Double)]), haming_distance: Int, matchMode: String, debugMode: String): Iterator[(Long, Long)] = {

    if(debugMode.equalsIgnoreCase("debug")){
      println("groupSimhashFlatMap " + iterTuple._1)
    }
    val iter_list = iterTuple._2.toList
    if(debugMode.equalsIgnoreCase("debug")){
      println(iter_list.map(x => x._1).mkString(","))
    }

    val list = iter_list.map(x => (java.lang.Long.valueOf(x._1), x._2.map(x => (java.lang.Long.valueOf(x._1), java.lang.Integer.valueOf(1), java.lang.Double.valueOf(x._3))).toList.asJava)).asJava
    val simList = SimhashFactory.getSimWeightPairsFromList(list, haming_distance, matchMode, debugMode).asScala.map(x => (x._1.toLong, x._2.toLong))
    if(debugMode.equalsIgnoreCase("debug") && simList.length > 0) {
      println("simList: " + simList.mkString(","))
    }
    simList.iterator
  }



  /*
    计算绝对前缀并分发数据
   */
  def prefixFlatMap(row: Row, threshold: Double, debugMode: String): Iterator[(Int, (Long, Array[(Int, String, Double)], Double))] = {
    val tokens_origin = row.getAs[SparseVector]("idf_features")
    val words = row.getAs[Seq[String]]("words")
    val combineBuffer = new ArrayBuffer[(Int, String, Double)]()
    for (i <- 0 until tokens_origin.indices.length){
      combineBuffer.+=((tokens_origin.indices.apply(i), words.apply(i), tokens_origin.values.apply(i)))
    }
    val idf_tokens = combineBuffer.toArray.sortWith((x,y) => x._3 > y._3)
    var total_weight = 0.0
    for (x <- idf_tokens){
      total_weight += x._3
    }
    val id = row.getLong(0)
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
      println("id=" + id + " tokenLen=" + idf_tokens.length + " prefixLen=" + (index+1) + " total_weight=" + total_weight + " prefix_weight=" + prefix_weight)
    }
//    println("idf_tokens " + idf_tokens.mkString(","))
    val arrayBuffer = new ArrayBuffer[(Int, (Long, Array[(Int, String, Double)], Double))]
    for(i <- 0 to Math.min(index, idf_tokens.length - 1)){
      arrayBuffer.append((idf_tokens.apply(i)._1, (id, idf_tokens, total_weight)))
    }
    arrayBuffer.iterator
  }

  /*
  计算句子长度和前缀长度
   */

  //  def prefixFlatMap(row: Row, threshold: Double): Row = {
  //    val tokens = row.getAs[SparseVector]("features").indices
  //    val id = row.getLong(0)
  //    val prefix = scala.math.ceil(tokens.length * threshold + Double.MinPositiveValue).toInt
  //    val new_row = Row(row).
  //    new_row
  //  }

  def flatMapSimlarityBuffer(setBuffer: (Int, SimlaritySetBuffer)): Iterator[Set[Long]] = {
    val arrayBuffer = new ArrayBuffer[Set[Long]]()
    for (simSet <- setBuffer._2.setBuffer) {
      if (simSet.simBuffer.size > 0) {
        val set: Set[Long] = Set(simSet.parentId).|(simSet.simBuffer.map(x => x._1).toSet)
        arrayBuffer.append(set)
      }
    }
    arrayBuffer.iterator
  }

}
