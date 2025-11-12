package org.example

import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.self.MinHashLSH
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.sys.process._

object SimJoinWithMinhashLsh {

  def main(args: Array[String]): Unit = {




    var num_hashFeature = 5000
    var threshold_of_distance = 0.1
    var numPartitions = 1

    var input = "input/00001.txt"
    var sim_output = "output/sim_output"
    var dup_output = "output/duplication_output"
    var result_output = "output/result_output"
    var index_output = "output/index_output"
    var wordcount_output = "output/wordcount_output"
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
    if(args.length == 10){
      input = args(0)
      sim_output = args(1)
      dup_output = args(2)
      result_output = args(3)
      threshold_of_distance = args(5).toFloat
      numPartitions = args(6).toInt
      num_hashFeature = args(7).toInt
      conf = new SparkConf()
        .registerKryoClasses(Array(classOf[JiebaSegmenter]))
        .setAppName("SimJoinWithMinhashLsh_" + num_hashFeature)
        .set("spark.sql.shuffle.partitions", args(4))
      index_output = args(8)
      wordcount_output = args(9)
    } else {
      val cmd = "rm -rf " + sim_output + " ; rm -rf " + dup_output + " ; rm -rf " + result_output + " ; rm -rf " + index_output + " ; rm -rf " + wordcount_output
      cmd.!!
    }

//    println("version 1.0 input = " + input + " output = " + sim_output + " threshold_distance = " + threshold_of_distance)
    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val timeBegin = System.currentTimeMillis()
    val segmenter = new JiebaSegmenter


    val spark=SparkSession.builder.getOrCreate()

    //读取文件，如果一个文件大小不超过hdfs 的一个 block，则一个文件的所有行
    // 都只在一个 partition 中，且一个 partition 只包含一个文件内容
    val lines = sc.textFile(input).repartition(numPartitions)
    // stage1 读文件分区，shuffle写


//    println("lines: ")
//    lines.take(3).foreach(println)
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
    origin_df.show(10, 150)

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
    val ids_tokenss_split = lines_ids.map(x => (x._2, segmenter.sentenceProcess(x._1).toArray))

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

    ids_hashingTf_df.show(20, 150)

    ids_hashingTf_df.persist(StorageLevel.MEMORY_AND_DISK)

    ids_hashingTf_df = ids_hashingTf_df.drop("words")

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

    // 根据 hashTF feature 计算 minHash，并使用 LSH 方法计算 minHash 间距离
    val mh = new MinHashLSH().setInputCol("features").setOutputCol("hashes").setNumHashTables(10)
    val model = mh.fit(ids_hashingTf_df)

    val sim_df = model.approxSimilarityJoin(ids_hashingTf_df, ids_hashingTf_df, threshold_of_distance, distCol="JaccardDistance")
      .select(col("datasetA.id").alias("idA"),
        col("datasetB.id").alias("idB"),
        col("JaccardDistance")
      ).filter("idA<idB")
//    sim_df.show(20)

    //    val result_df = origin_df.join(sim_df, Seq("id"), "left")
//    println("origin_df.count(): " + origin_df.count())
//    println("sim_df.count(): " + sim_df.count())
    //    println("result_df.count(): " + result_df.count())
    //    result_df.show(10)

    import spark.implicits._
//    val sim_df_detail_df = sim_df.join(ids_tokenss_df.alias("tableA"), $"idA" === $"id", "left")
//      .join(ids_tokenss_df.alias("tableB"), $"idB" === $"tableB.id", "left")
//      .select(sim_df("idA"), sim_df("idB"), sim_df("JaccardDistance"), col("tableA.words").alias("wordsA"), col("tableB.words").alias("wordsB"))

//    sim_df_detail_df.show(10)
    sim_df.write.csv(sim_output)

    val timeEnd = System.currentTimeMillis()

    println("time cost: " + (timeEnd - timeBegin))
    spark.close()
    sc.stop()

  }

}
