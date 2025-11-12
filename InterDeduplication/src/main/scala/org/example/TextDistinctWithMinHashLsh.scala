package org.ml

import com.huaban.analysis.jieba.JiebaSegmenter
import component.TextDistinct
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.self.MinHashLSH
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet
import scala.io.Source
import scala.sys.process._

object TextDistinctWithMinHashLsh {

  def main(args: Array[String]): Unit = {


    val num_hashFeature = 5000
    var distance = 0.3
    val numPartitions = 1

    var input = "input/mini.txt"
    var output = "output"
    var conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .setAppName("TextDistinctWithMinHashLsh_" + num_hashFeature  + "_" + distance.formatted("%.1f"))
      .setMaster("local")
      .set("spark.sql.shuffle.partitions","4")
//      .setMaster("spark://stardustdeMacBook-Pro.local:7077")
//      .set("spark.driver.memory", "512m")
//      .set("spark.driver.cores", "1")
//      .set("spark.executor.cores", "1")
//      .set("spark.executor.memory", "512m")
    if(args.length == 4){
      input = args(0)
      output = args(1)
      conf = new SparkConf()
        .registerKryoClasses(Array(classOf[JiebaSegmenter]))
        .setAppName("CodeCount Test")
        .set("spark.sql.shuffle.partitions", args(2))
      distance = args(3).toFloat
    }

    val cmd = "rm -rf " + output
    cmd.!!


    println("version 1.0 input = " + input + " output = " + output)
    //创建SparkContext对象
    val sc = new SparkContext(conf)


    val segmenter = new JiebaSegmenter


    val spark=SparkSession.builder.getOrCreate()

    //读取文件，如果一个文件大小不超过hdfs 的一个 block，则一个文件的所有行
    // 都只在一个 partition 中，且一个 partition 只包含一个文件内容
    var lines = sc.textFile(input).repartition(numPartitions)
    // stage1 读文件分区，shuffle写


    println("lines: ")
    lines.take(3).foreach(println)
    // stage2 shuffle读、filter过滤、zipWithIndex编号、map分词、RDD转DataFrame、

    // 加上 ID
    val lines_ids = lines.zipWithIndex().filter(x => x._1.length > 10).persist(StorageLevel.MEMORY_AND_DISK)
    println("lines_ids.take(100) ")
    lines_ids.take(10).foreach(println)

    val lines_id_schema: StructType = StructType(Array(
      StructField("id", LongType),
      StructField("words", StringType)
      ),
    )

    val origin_rdd_row = lines_ids.map(x => Row(x._2, x._1))

    val origin_df = spark.createDataFrame(origin_rdd_row, lines_id_schema)

    val pattern = """[^\u4e00-\u9fa5]""".r
    var stopSet = HashSet[String]()
    val stopWordsFile = Source.fromResource("cn_stopwords.txt")
    stopWordsFile.getLines().foreach(stopSet += _)
    stopSet.take(10).foreach(println)


    val ids_tokenss_split = lines_ids.map(x => (x._2, segmenter.sentenceProcess(x._1).toArray))

    println("id_tokenss_split")
    ids_tokenss_split.take(3).foreach(x => println(x._1 + " : " + x._2.mkString(",")))

    val ids_tokenss =  ids_tokenss_split.map(x => Row(x._1, x._2.map(_.toString).filter(!stopSet.contains(_))))

    println("ids_tokenss.take(100)")
    ids_tokenss.take(3).foreach(x => println(x.get(0) + " : " + x.getAs[Array[String]](1).mkString(",")))


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

    val ids_tokenss_hashTFFeatures_df = hashingTF.transform(ids_tokenss_df)

    ids_tokenss_hashTFFeatures_df.persist(StorageLevel.MEMORY_AND_DISK)

    // 根据 hashTF feature 计算 minHash，并使用 LSH 方法计算 minHash 间距离
    val mh = new MinHashLSH().setInputCol("features").setOutputCol("hashes").setNumHashTables(10)
    val model = mh.fit(ids_tokenss_hashTFFeatures_df)

    val sim_df = model.approxSimilarityJoin(ids_tokenss_hashTFFeatures_df, ids_tokenss_hashTFFeatures_df, distance, distCol="JaccardDistance")
      .select(col("datasetA.id").alias("idA"),
      col("datasetB.id").alias("idB"),
      col("JaccardDistance")
      ).filter("idA<idB")
    sim_df.show(10)

//    val result_df = origin_df.join(sim_df, Seq("id"), "left")
    println("origin_df.count(): " + origin_df.count())
    println("sim_df.count(): " + sim_df.count())
//    println("result_df.count(): " + result_df.count())
//    result_df.show(10)

    import spark.implicits._
    val sim_df_detail_df = sim_df.join(ids_tokenss_df.alias("tableA"), $"idA" === $"id", "left")
      .join(ids_tokenss_df.alias("tableB"), $"idB" === $"tableB.id", "left")
      .select(sim_df("idA"), sim_df("idB"), sim_df("JaccardDistance"), col("tableA.words").alias("wordsA"), col("tableB.words").alias("wordsB"))

    sim_df_detail_df.show(10)
    sim_df.write.csv(output + "/sim_output")
    val sim_rdd = sim_df.rdd.map(x => (x.getLong(0), x.getLong(1)))
    /*
      work point: 保存重复记录和去重结果
     */
    val textDistinct = new TextDistinct(output)
    textDistinct.saveDeduplicateLineResult(sim_rdd, lines_ids, spark, sc)
    sc.stop()
  }

  def simSetFlatMap(pair: (Long, Long)): Iterator[(Long, Set[Long])] = {
    val pair_set = Set(pair._1, pair._2)
    val list = Array((pair._1, pair_set), (pair._2, pair_set))
    list.iterator
  }

}
