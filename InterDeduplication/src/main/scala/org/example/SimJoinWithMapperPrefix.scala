package org.example

import com.huaban.analysis.jieba.JiebaSegmenter
import javax.swing.JToolBar.Separator
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.self.MinHashLSH
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.sys.process._
import scala.util.control._


class SimlaritySetBuffer(initId: Long, initArray: Array[Int], threshold: Double){
  var setBuffer: ArrayBuffer[SimlaritySet] = ArrayBuffer(new SimlaritySet(initId, initArray, threshold))


  def addRecord(otherId: Long, otherArray: Array[Int]): Unit = {
    val loop = new Breaks
    var flag = false
    loop.breakable {
      /*
        经过prefix筛选后，map端能判断相似的可以先压缩到parent，不能相似的暂时不能丢弃，因为不确定其它分区是否有相似的
       */
      for (simlaritySet <- setBuffer) {
        val joinResult = simlaritySet.addRecord(otherId, otherArray)
        if (joinResult) {
            flag = true
            loop.break()
        }
        if (!flag){
          val simlaritySet = new SimlaritySet(otherId, otherArray, threshold)
          setBuffer.append(simlaritySet)
          println("SimlaritySetBuffer.appendBuffer:\n" + this.toString)
        }
      }

    }

    }

  def addBuffer(otherBuffer: SimlaritySetBuffer) = {
      for (leftSet <- this.setBuffer){
        for (rightSet <- otherBuffer.setBuffer) {
            val isMerge = leftSet.mergeSet(rightSet)
            if(!isMerge){
              this.setBuffer.append(rightSet)
              println("SimlaritySetBuffer.addBuffer2:\n" + this.toString)
            }
        }
      }
  }

  @Override
  override def toString: String = {
    var stringBuffer = new StringBuffer("SimlaritySetBuffer:\n")
    for (buffer <- this.setBuffer){
      stringBuffer.append(buffer.toString() + "\n")
    }
    stringBuffer.toString

  }
}

class SimlaritySet(initId: Long, initArray: Array[Int], thresholds: Double) {
  var parentArray: Array[Int] = initArray
  var parentId = initId
  var simBuffer: ArrayBuffer[(Long,Double)] = ArrayBuffer()
  var distance : Double = thresholds

  def addRecord(otherId: Long, otherArray: Array[Int]): Boolean = {
      val simlarity = SimJoinWithMapperPrefix.distanceCal(parentArray, otherArray)
      if (simlarity < distance){
          simBuffer.append((otherId, simlarity))
        println("simBuffer append: " + this.toString())
          true
      } else {
          false
      }
  }

  def mergeSet(otherSet: SimlaritySet): Boolean = {
    val simlarity = SimJoinWithMapperPrefix.distanceCal(this.parentArray, otherSet.parentArray)
    if(simlarity < distance){
      this.simBuffer.append((otherSet.parentId, simlarity))
      for (tuple <- otherSet.simBuffer){
        this.simBuffer.append(tuple)
      }
      println("merge set: " + this.toString())
      true
    } else {
      false
    }
  }

  @Override
  override def toString(): String ={
    "parentId=" + parentId + " parentArr=" + parentArray.mkString(",")  + " threshold=" + distance+ " simBuffer=" + simBuffer.mkString(",")
  }
}



object SimJoinWithMapperPrefix {


  var distance = 0.1

  def distanceCal(leftArray: Array[Int], rightArray: Array[Int]): Double ={
    val interSize = leftArray.intersect(rightArray).size
    val unionSize = leftArray.size + rightArray.size - interSize
    val result = 1 - interSize * 1.0 / unionSize
    println("leftArray: " + leftArray.mkString(",") + "  rightArray: " + rightArray.mkString(","))
    println("interSize: " + interSize + " unionSize: " + unionSize + " result: " + result)
    result
  }

  def main(args: Array[String]): Unit = {

    val num_hashFeature = 5000
    var numPartitions = 1

    var input = "input/00001.txt"
    var sim_output = "output/sim_output"
    var dup_output = "output/duplication_output"
    var result_output = "output/result_output"
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
    if(args.length == 7){
      input = args(0)
      sim_output = args(1)
      dup_output = args(2)
      result_output = args(3)
      conf = new SparkConf()
        .registerKryoClasses(Array(classOf[JiebaSegmenter]))
        .setAppName("CodeCount Test")
        .set("spark.sql.shuffle.partitions", args(4))
      distance = args(5).toFloat
      numPartitions = args(6).toInt
    } else {
      val cmd = "rm -rf " + sim_output + " ; rm -rf " + dup_output+ " ; rm -rf " + result_output
      cmd.!!
    }

    println("version 1.0 input = " + input + " output = " + sim_output)
    //创建SparkContext对象
    val sc = new SparkContext(conf)


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
    val lines_ids = lines.zipWithIndex().filter(x => x._1.length > 8).persist(StorageLevel.MEMORY_AND_DISK)
    //    println("lines_ids.take(3) ")
    //    lines_ids.take(3).foreach(println)

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
    stopSet.take(10).foreach(println)

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

    ids_hashingTf_df = ids_hashingTf_df.drop("words")

    ids_hashingTf_df.show(20, 150)

//    val ds = ids_hashingTf_df.as[(Long, String)]
//    ds.flatMap {
//      case (x1, x2, x3, x4) => x3.split(",").map((x1, x2, _, x4))
//    }.toDF

    val words_prefix = ids_hashingTf_df.rdd.flatMap(prefixFlatMap(_, distance))
    words_prefix.take(20).foreach(x => println("wordsPrefix= " + x._1 + " -> " +  x._2._1 + ",[" + x._2._2.mkString(",") + "]"))
    val sim_rdd = words_prefix.combineByKey(createCombiner, mergeValue, mergeCominer)
    sim_rdd.take(20).foreach(println)
    val sim_set_rdd = sim_rdd.flatMap(flatMapSimlarityBuffer)
    sim_set_rdd.take(20).foreach(println)
//    sim_rdd.map(x => x._2.)



  }

  def createCombiner(value: (Long, Array[Int])): SimlaritySetBuffer ={
    new SimlaritySetBuffer(value._1, value._2, SimJoinWithMapperPrefix.distance)
  }

  def mergeValue(buffer: SimlaritySetBuffer, value: (Long, Array[Int])): SimlaritySetBuffer = {
    buffer.addRecord(value._1, value._2)
    buffer
  }

  def mergeCominer(bufferLeft: SimlaritySetBuffer, bufferRight: SimlaritySetBuffer): SimlaritySetBuffer = {
    bufferLeft.addBuffer(bufferRight)
    bufferLeft
  }

  def prefixFlatMap(row: Row, threshold: Double): Iterator[(Int, (Long, Array[Int]))] = {
      val tokens = row.getAs[SparseVector]("features").indices
      val id = row.getLong(0)
      val prefix = scala.math.ceil(tokens.length * threshold + Double.MinPositiveValue).toInt
      val arrayBuffer = new ArrayBuffer[(Int, (Long, Array[Int]))]
      for(i <- 0 to prefix - 1){
        arrayBuffer.append((tokens.apply(i), (id, tokens)))
      }
      arrayBuffer.iterator
  }

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
