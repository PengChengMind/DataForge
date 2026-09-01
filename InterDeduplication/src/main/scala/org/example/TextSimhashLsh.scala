package org.example

import breeze.math.Complex.ComplexZero.zero.re
import breeze.stats.mode
import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.self.MinHashLSH
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.math.Ordered.orderingToOrdered

object TextSimhashLsh {

  def main(args: Array[String]): Unit = {


    val num_hashFeature = 5000
    val threshold_of_distance = 0.1
    val numPartitions = 1

    var input = "input/mini.txt"
    var output = "output/duplication_output"
    var conf = new SparkConf()
      .setAppName("MinHashLsh Test")
      .setMaster("local")
      .set("spark.sql.warehouse.dir","file:/Users/stardust/StardustScalaProjects/SparkScalaProjects/spark-warehouse")
    if(args.length == 2){
      input = args(0)
      output = args(1)
      conf = new SparkConf()
        .setAppName("MinHashLsh Test")
    }

    println("version 1.0 input = " + input + " output = " + output)
    //创建SparkContext对象
    val sc = new SparkContext(conf)




    val spark=SparkSession.builder.getOrCreate()

    //读取文件，如果一个文件大小不超过hdfs 的一个 block，则一个文件的所有行
    // 都只在一个 partition 中，且一个 partition 只包含一个文件内容
    var lines = sc.textFile(input).repartition(numPartitions)
    // stage1 读文件分区，shuffle写


    println(lines.take(100))
    lines = lines.filter(x => x.length > 3).persist(StorageLevel.MEMORY_AND_DISK)
    // stage2 shuffle读、filter过滤、zipWithIndex编号、map分词、RDD转DataFrame、

    // 加上 ID
    val lines_ids = lines.zipWithIndex()
    println("lines_ids.take(100) ")
    lines_ids.foreach(println)

    val tempPath = "/Users/stardust/PclPythonProjects/pythonProject/output/jiebaFile"
    val jiebaFile = sc.textFile(tempPath).zipWithIndex().map(x => Row(x._2,Array(x._1.split(","))))
    jiebaFile.foreach(println)




    val schema: StructType = StructType(Array(
      StructField("id", LongType),
      StructField("words", ArrayType(StringType)
      )))

//    val segmenter = new JiebaSegmenter
//    val ids_tokenss = lines_ids.map(x => Row((x._2, segmenter.sentenceProcess(x._1))))

    // RDD 转 DataFrame
    val ids_tokenss_df = spark.createDataFrame(jiebaFile, schema)

    ids_tokenss_df.show(100)


    //计算 hashTF feature
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("features")
      .setNumFeatures(num_hashFeature)

    val ids_tokenss_hashTFFeatures_df = hashingTF.transform(ids_tokenss_df).repartition(888)

    //    ids_tokenss_hashTFFeatures_df.persist(StorageLevel.MEMORY_AND_DISK)

    // 根据 hashTF feature 计算 minHash，并使用 LSH 方法计算 minHash 间距离
    val mh = new MinHashLSH().setInputCol("features").setOutputCol("hashes").setNumHashTables(10)
    val model = mh.fit(ids_tokenss_hashTFFeatures_df)

    val idA_idB = model.approxSimilarityJoin(ids_tokenss_hashTFFeatures_df, ids_tokenss_hashTFFeatures_df, threshold_of_distance, distCol="JaccardDistance")
      .select(col("datasetA.id").alias("idA"),
        col("datasetB.id").alias("idB"),
        col("JaccardDistance"))
    idA_idB.write.csv(output)
  }

}
