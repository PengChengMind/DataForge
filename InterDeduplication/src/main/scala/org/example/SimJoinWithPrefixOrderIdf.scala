package org.example

import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import algorithm.simhash.SimhashFactory
import org.utils.Tools

import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.sys.process._
import scala.util.control.Breaks

object SimJoinWithPrefixOrderIdf {




  def main(args: Array[String]): Unit = {


    val timeBegin = System.currentTimeMillis()


    var distance = 0.5

    var num_hashFeature = 5000
    var numPartitions = 1
    var groupPartitions = 4

    var input = "input/input_3.txt"
    var index_output = "output/index_output"
    var wordcount_output = "output/wordcount_output"
    var sim_output = "output/sim_output"
    var dup_output = "output/duplication_output"
    var result_output = "output/result_output"

    var hashing_output = "output/hasing_output"
    var conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .setAppName("Scala word analysis Test")
      .setMaster("local")
      .set("spark.sql.shuffle.partitions","4")
    //      .setMaster("spark://stardustdeMacBook-Pro.local:7077")
    //      .set("spark.driver.memory", "512m")
    //      .set("spark.driver.cores", "1")
    //      .set("spark.executor.cores", "1")
    //      .set("spark.executor.memory", "512m")
    if(args.length == 12){
      input = args(0)
      sim_output = args(1)
      dup_output = args(2)
      result_output = args(3)
      distance = args(5).toFloat
      numPartitions = args(6).toInt
      groupPartitions = args(7).toInt
      wordcount_output = args(8)
      index_output = args(9)
      num_hashFeature = args(10).toInt
      hashing_output = args(11)
      conf = new SparkConf()
        .registerKryoClasses(Array(classOf[JiebaSegmenter]))
        .setAppName("SimJoinWithPrefixOrderIdf_" + num_hashFeature + "_" + distance.formatted("%.1f"))
        .set("spark.sql.shuffle.partitions", args(4))
    } else {
      val cmd = "rm -rf " + sim_output + " ; rm -rf " + dup_output+ " ; rm -rf " + result_output+ " ; rm -rf " + wordcount_output + " ; rm -rf " + index_output + " ; rm -rf " + hashing_output
      cmd.!!
    }

    println("version 1.0 input = " + input + " output = " + sim_output + " threshold_distance = " + distance)
    //创建SparkContext对象
    val sc = new SparkContext(conf)


    val segmenter = new JiebaSegmenter


    val spark=SparkSession.builder.getOrCreate()

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

    val lines_id_schema: StructType = StructType(Array(
      StructField("id", LongType),
      StructField("words", StringType)
    ),
    )

    val origin_rdd_row = lines_ids.map(x => Row(x._2, x._1))

    val origin_df = spark.createDataFrame(origin_rdd_row, lines_id_schema)
    //    origin_df.show(10, 150)

    val pattern = """[^\u4e00-\u9fa5]""".r
    var stopSet = HashSet[String]()
    val stopWordsFile = Source.fromResource("cn_stopwords.txt")
    stopWordsFile.getLines().foreach(stopSet += _)
    //    stopSet.take(10).foreach(println)

    // 删除 \\n;  分词
    //    val grop = """\n""".r
    //    val string = "我爱中国的网球王子还是什么"
    //    println(grop.replaceAllIn(string, ""))
    //    println("test jieba: " +  segmenter.sentenceProcess(pattern.replaceAllIn(string, "")))
    lines_ids.map(x => (x._2, segmenter.sentenceProcess(x._1)))
    val ids_tokenss_split = lines_ids.map(x => (x._2, segmenter.sentenceProcess(pattern.replaceAllIn(x._1, "")).toArray))

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

    var ids_hashingTf_df = hashingTF.transform(ids_tokenss_df)


    val idf = new IDF().setInputCol("features").setOutputCol("idf_features")
    val idfModel = idf.fit(ids_hashingTf_df)
    val rescaledData = idfModel.transform(ids_hashingTf_df)

    rescaledData.show(20, 150)

    val ids_hasing_save = ids_hashingTf_df.rdd.map(x => (x.getAs[Long]("id"), x.getAs[String]("words"), x.getAs[SparseVector]("features").indices.mkString(",")))

    //    ids_hashingTf_df.show(20, 150)
    //    ids_hasing_save.take(100).foreach(println)
    ids_hasing_save.saveAsTextFile(hashing_output)

    ids_hashingTf_df = ids_hashingTf_df.drop("words")

    //    println("ids_hashingTf_df.count() = " + ids_hashingTf_df.count())

    //    val ds = ids_hashingTf_df.as[(Long, String)]
    //    ds.flatMap {
    //      case (x1, x2, x3, x4) => x3.split(",").map((x1, x2, _, x4)):
    //    }.toDF

    /*
      词频统计
     */
    val word_count_rdd = ids_hashingTf_df.rdd.flatMap(_.getAs[SparseVector]("features").indices)  //运算步骤
      .map((_,1))
      .reduceByKey(_+_)
      .sortBy(_._2,false)
    //    println("words count")
    //    word_count_rdd.take(20).foreach(println)
    word_count_rdd.saveAsTextFile(wordcount_output)

    val wordscountMap = word_count_rdd.collectAsMap()
    val wordscountBroad = sc.broadcast(wordscountMap)

    val flat_perfix_rdd = rescaledData.rdd
      .flatMap(prefixFlatMap(_, distance))
        flat_perfix_rdd.take(20).foreach(x => println("wordsPrefix= " + x._1 + " -> " +  x._2._1 + ",[" + x._2._2.mkString(",") + "]"))
        flat_perfix_rdd.take(20).foreach(x => println("token = " + x._1 + " -> record_id=" +  x._2._1));
    val group_rdd = flat_perfix_rdd
      .groupByKey(groupPartitions)

    val distinct_rdd = group_rdd
      .flatMap(groupFlatMap(_, distance))
      .distinct()

        Tools.printPartitionNum("ids_hashingTf_df: ", ids_hashingTf_df.rdd)
        Tools.printPartitionNum("flat_perfix_rdd: ", flat_perfix_rdd)
        Tools.printGroupPartitionNum("group_rdd elements: ", group_rdd)
        Tools.printPartitionNum("distinct_rdd elements: ", distinct_rdd)
    distinct_rdd.take(20).foreach(println)
    val timeEnd = System.currentTimeMillis()

    println("time cost: " + (timeEnd - timeBegin))
    distinct_rdd.saveAsTextFile(sim_output)
    //    sim_rdd.map(x => x._2.)



  }


  /*
    利用flatMap逐条处理group后的数据，每条数据都是一个token对应的多条记录的迭代器，避免整个分区的数据自连接
    内部自连接验证，相对于两表连接，减少一半比较次数和shuffle量。还可以尝试写入其它剪枝
   */
  def groupFlatMap(iterTuple: (Int, Iterable[(Long, Array[(Int, Double)], Double)]), distance: Double): Iterator[(Long, Long)] = {

    val list = iterTuple._2.toList
    var index_i = 0
    var cmp_count = 0
    val arrayBuffer = new ArrayBuffer[(Long, Long)]()
    while (index_i < list.size){
      val curTuple = list.apply(index_i)
      var index_j = index_i + 1
      while (index_j < list.size) {
        val nextTuple = list.apply(index_j)
        val tuple_distance = Tools.distanceCalInt(curTuple._2, nextTuple._2, curTuple._3, nextTuple._3)
//        println("tuple_distance=" + tuple_distance)
//        println("curId: " + curTuple._1 +  " curTuple: " + curTuple._2.mkString(","))
//        println("nextId: " + nextTuple._1 +  " nextTuple: " + nextTuple._2.mkString(","))
        cmp_count = cmp_count + 1
        if(tuple_distance < distance){
          arrayBuffer.append((curTuple._1, nextTuple._1))
//                      println("find sim: " + curTuple._1 + "," + nextTuple._1)
          //            println("curTuple2: " + curTuple._2.mkString(","))
          //            println("nextTuple2: " + nextTuple._2.mkString(","))
        }
        index_j = index_j + 1
      }
      index_i = index_i + 1
    }
//    println("groupFlatMap: cmp_count=" + cmp_count + " token=" + iterTuple._1 + " list.size=" + list.length + " part_no=" + TaskContext.getPartitionId())
    arrayBuffer.iterator
  }
  /*
    计算绝对前缀并分发数据
   */
  def prefixFlatMap(row: Row, threshold: Double): Iterator[(Int, (Long, Array[(Int, Double)], Double))] = {
    val tokens_origin = row.getAs[SparseVector]("idf_features")
    val combineBuffer = new ArrayBuffer[(Int, Double)]()
    for (i <- 0 until tokens_origin.indices.length){
      combineBuffer.+=((tokens_origin.indices.apply(i), tokens_origin.values.apply(i)))
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
    val arrayBuffer = new ArrayBuffer[(Int, (Long, Array[(Int, Double)], Double))]
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
