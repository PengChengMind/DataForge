package org.example

import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.utils.Tools

import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.sys.process._
import scala.util.control.Breaks


object IdfPrefixWordCount {




  def main(args: Array[String]): Unit = {


    val timeBegin = System.currentTimeMillis()


    var distance = 0.5

    var num_hashFeature = 5000000
    var numPartitions = 1
    var groupPartitionNum = 4

    var input = "input/mini.txt"
    var index_output = "output/index_output"
    var wordcount_output = "output/wordcount_output"
    var prefixcount_output = "output/prefixcount_output"
    var tokenprefix_output = "output/tokenprefix_output"
    var length_output = "output/length_output"

    var hashing_output = "output/hasing_output"
    var conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .setAppName("IdfWordCount")
      .setMaster("local")
      .set("spark.sql.shuffle.partitions","4")
    //      .setMaster("spark://stardustdeMacBook-Pro.local:7077")
    //      .set("spark.driver.memory", "512m")
    //      .set("spark.driver.cores", "1")
    //      .set("spark.executor.cores", "1")
    //      .set("spark.executor.memory", "512m")
    if(args.length == 12){
      var pos = 0
      input = args(pos)
      pos = pos + 1
      distance = args(pos).toFloat
      pos = pos + 1
      numPartitions = args(pos).toInt
      pos = pos + 1
      conf = new SparkConf()
        .registerKryoClasses(Array(classOf[JiebaSegmenter]))
        .setAppName("IdfWordCount_" + num_hashFeature + "_" + distance.formatted("%.1f"))
        .set("spark.sql.shuffle.partitions", args(pos))
      pos = pos + 1
      groupPartitionNum = args(pos).toInt
      pos = pos + 1
      num_hashFeature = args(pos).toInt
      pos = pos + 1
      wordcount_output = args(pos)
      pos = pos + 1
      index_output = args(pos)
      pos = pos + 1
      hashing_output = args(pos)
      pos = pos + 1
      prefixcount_output = args(pos)
      pos = pos + 1
      tokenprefix_output = args(pos)
      pos = pos + 1
      length_output = args(pos)
      pos = pos + 1
    } else {
      val cmd = "rm -rf " + prefixcount_output+ " ; rm -rf " + wordcount_output + " ; rm -rf " + index_output + " ; rm -rf " + hashing_output + " ; rm -rf " + tokenprefix_output + " ; rm -rf " + length_output
      cmd.!!
    }

    println("version 1.0 input = " + input + " threshold_distance = " + distance)
    //创建SparkContext对象
    val sc = new SparkContext(conf)


    val segmenter = new JiebaSegmenter


    val spark=SparkSession.builder.getOrCreate()
    import spark.implicits._

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

    lines_ids.saveAsTextFile(index_output)
    /*
      work point：统计文档总数
     */
    println("total document count = " + lines_ids.count())

    val lines_id_schema: StructType = StructType(Array(
      StructField("id", LongType),
      StructField("words", StringType)
    ),
    )

    val origin_rdd_row = lines_ids.map(x => Row(x._2, x._1))

    val origin_df = spark.createDataFrame(origin_rdd_row, lines_id_schema)
    //    origin_df.show(10, 150)

    /*
      work point：消除停止词，去掉非中文字符
     */
    val pattern = """[^\u4e00-\u9fa5]""".r
    var stopSet = HashSet[String]()
    val stopWordsFile = Source.fromResource("cn_stopwords.txt")
    stopWordsFile.getLines().foreach(stopSet += _)
    lines_ids.map(x => (x._2, segmenter.sentenceProcess(x._1)))
    val ids_tokenss_split = lines_ids.map(x => (x._2, segmenter.sentenceProcess(pattern.replaceAllIn(x._1, "")).toArray))
//    val ids_tokenss_split = lines_ids.map(x => (x._2, segmenter.sentenceProcess(x._1).toArray))

    //    println("id_tokenss_split")
    //    ids_tokenss_split.take(3).foreach(x => println(x._1 + " : " + x._2.mkString(",")))

    val ids_tokenss =  ids_tokenss_split.map(x => Row(x._1, x._2.map(_.toString).filter(!stopSet.contains(_))))

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


    val idf = new IDF().setInputCol("features").setOutputCol("idf_features")
    val idfModel = idf.fit(ids_hashingTf_df)
    val rescaledData = idfModel.transform(ids_hashingTf_df)

    rescaledData.show(20, 150)

    val ids_hasing_save = ids_hashingTf_df.rdd.map(x => (x.getAs[Long]("id"), x.getAs[String]("words"), x.getAs[SparseVector]("features").indices.mkString(",")))
    ids_hasing_save.saveAsTextFile(hashing_output)
    /*
      work point: hashingTf词频统计
     */
    val word_count_rdd = ids_hashingTf_df.rdd.flatMap(_.getAs[SparseVector]("features").indices)  //运算步骤
      .map((_,1))
      .reduceByKey(_+_)
      .sortBy(_._2,false)
    //    println("words count")
    //    word_count_rdd.take(20).foreach(println)
    word_count_rdd.saveAsTextFile(wordcount_output)

    /*
      work point: 词表大小
     */
    println("total word number=" + word_count_rdd.count())

    val wordscountMap = word_count_rdd.collectAsMap()
    val wordscountBroad = sc.broadcast(wordscountMap)

    /*
      work point: 统计文本长度分布
     */
    val code :(SparseVector => Int) = (arg: SparseVector) => {arg.indices.length}
    val addCol = udf(code)
    val word_length_rdd = ids_hashingTf_df.withColumn("length", addCol(col("features"))).groupBy("length").count().orderBy(col("length").desc)
    word_length_rdd.show()
    word_length_rdd.write.csv(length_output)

    /*
    work point: 统计token前缀词频分布
     */
    val token_flat_df = rescaledData
      .flatMap(tokenPrefixFlatMap(_, distance))
    token_flat_df.show(100, 100)
    val token_group_df = token_flat_df
      .groupBy("_1")
      .agg("_1" -> "count", "_4" -> "max")
      .orderBy($"count(_1)".desc)
    //    flat_perfix_rdd.take(20).foreach(x => println("wordsPrefix= " + x._1 + " -> " +  x._2._1 + ",[" + x._2._2.mkString(",") + "]"))
    //    flat_perfix_rdd.take(20).foreach(x => println("token = " + x._1 + " -> record_id=" +  x._2._1));

    token_group_df.show(100, 100)
    token_group_df.write.csv(tokenprefix_output)
    
    
    /*
      work point: 统计idf前缀词频分布
     */
    val idf_flat_df = rescaledData
      .flatMap(idfPrefixFlatMap(_, distance))
    idf_flat_df.show(100, 100)
    val idf_group_df = idf_flat_df
      .groupBy("_1")
      .agg("_1" -> "count", "_4" -> "max")
      .orderBy($"count(_1)".desc)
    //    flat_perfix_rdd.take(20).foreach(x => println("wordsPrefix= " + x._1 + " -> " +  x._2._1 + ",[" + x._2._2.mkString(",") + "]"))
    //    flat_perfix_rdd.take(20).foreach(x => println("token = " + x._1 + " -> record_id=" +  x._2._1));

    idf_group_df.show(100, 100)
    idf_group_df.write.csv(prefixcount_output)

    val timeEnd = System.currentTimeMillis()

    println("time cost: " + (timeEnd - timeBegin))
  }

  /*
    计算idf绝对前缀
   */
  def idfPrefixFlatMap(row: Row, threshold: Double): Iterator[(Int, Long, Seq[String], String)] = {
    val tokens_origin = row.getAs[SparseVector]("idf_features")
    val tokens_words = row.getAs[Seq[String]]("words")
    val combineBuffer = new ArrayBuffer[(Int, Double, String)]()
    for (i <- 0 until tokens_origin.indices.length){
      combineBuffer.+=((tokens_origin.indices.apply(i), tokens_origin.values.apply(i), tokens_words.apply(i)))
    }
    val idf_tokens = combineBuffer.toArray.sortWith((x,y) => x._2 > y._2)
    var total_weight = 0.0
    for (x <- idf_tokens){
      total_weight += x._2
    }
    val id = row.getLong(0)
    val prefix_weight = total_weight * threshold
    var sum_weight = 0.0
    val loop = new Breaks
    var index = 0
    loop.breakable {
      while (index < idf_tokens.length) {
        sum_weight += idf_tokens.apply(index)._2
        if (sum_weight > prefix_weight) {
          loop.break()
        }
        index = index + 1
      }
    }
    //    println("id=" + id + " tokenLen=" + idf_tokens.length + " prefixLen=" + (index+1) + " total_weight=" + total_weight + " prefix_weight=" + prefix_weight)
    //    println("idf_tokens " + idf_tokens.mkString(","))
    val arrayBuffer = new ArrayBuffer[(Int, Long, Seq[String], String)]
    for(i <- 0 to Math.min(index, idf_tokens.length - 1)){
      arrayBuffer.append((idf_tokens.apply(i)._1, id, tokens_words, idf_tokens.apply(i)._3))
    }
    arrayBuffer.iterator
  }


  /*
    计算单词绝对前缀
   */
  def tokenPrefixFlatMap(row: Row, threshold: Double): Iterator[(Int, Long, Seq[String], String)] = {
    val tokens_origin = row.getAs[SparseVector]("idf_features")
    val tokens_words = row.getAs[Seq[String]]("words")
    val combineBuffer = new ArrayBuffer[(Int, Double, String)]()
    for (i <- 0 until tokens_origin.indices.length){
      combineBuffer.+=((tokens_origin.indices.apply(i), tokens_origin.values.apply(i), tokens_words.apply(i)))
    }
    val idf_tokens = combineBuffer.toArray.sortWith((x,y) => x._2 > y._2)
    var total_weight = 0.0
    for (x <- idf_tokens){
      total_weight += x._2
    }
    val id = row.getLong(0)
    val prefix = scala.math.ceil(idf_tokens.indices.length * threshold + Double.MinPositiveValue).toInt

    //    println("id=" + id + " tokenLen=" + idf_tokens.length + " prefixLen=" + (index+1) + " total_weight=" + total_weight + " prefix_weight=" + prefix_weight)
    //    println("idf_tokens " + idf_tokens.mkString(","))
    val arrayBuffer = new ArrayBuffer[(Int, Long, Seq[String], String)]
    for (i <- 0 until Math.min(prefix, idf_tokens.length)) {
      arrayBuffer.append((idf_tokens.apply(i)._1, id, tokens_words, idf_tokens.apply(i)._3))
    }
    arrayBuffer.iterator
  }

}
