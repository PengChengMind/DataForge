package component

import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.utils.Tools
import tokenizer.ITokenizer

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.io.Source
import scala.util.matching.Regex

class TextStatistics(rootPath: String, stopWordsPath: String = "no_stopwords.txt", langMode: String = "all", tokenizer: ITokenizer, debugMode: String) extends Serializable {
  // 解析stopwords
  val logger = LoggerFactory.getLogger(this.getClass)

  def getStopSetFromPath(path: String): HashSet[String] = {
    var stopSet = HashSet[String]()
    val stopWordsFile = Source.fromResource(path)("UTF-8")
    stopWordsFile.getLines().foreach(stopSet += _)
    stopSet
  }

  def getStopSet(): Set[String] = {
    var stopSet = Set[String]()
    if(!langMode.equalsIgnoreCase("cn")){
      val stopWordsFile = Source.fromResource("en_stopwords.txt")("UTF-8")
      stopWordsFile.getLines().foreach(stopSet += _)
    }
    if(!langMode.equalsIgnoreCase("en")){
      val stopWordsFile = Source.fromResource("cn_stopwords.txt")("UTF-8")
      stopWordsFile.getLines().foreach(stopSet += _)
    }
    stopSet
  }


  def getLangRegex(): Regex = {
    if ("cn".equals(langMode)) {
      """[^\u4e00-\u9fa5]""".r
    } else if ("en".equals(langMode)) {
      """[^a-zA-Z]""".r
    } else if("common".equalsIgnoreCase(langMode)){
      """[^\u4e00-\u9fa5_a-zA-Z0-9]""".r
    } else {
      """[\pP\pS]""".r
    }
  }

  def getLangRegex(specificLanguage: String): Regex = {
    if ("cn".equals(specificLanguage)) {
      """[^\u4e00-\u9fa5]""".r
    } else if ("en".equals(specificLanguage)) {
      """[^a-zA-Z]""".r
    } else if ("cn_en".equals(specificLanguage)) {
      """[^\u4e00-\u9fa5_a-zA-Z]""".r
    } else if("common".equalsIgnoreCase(specificLanguage)){
      """[^\u4e00-\u9fa5_a-zA-Z0-9]""".r
    } else {
      """[\pP\pS]""".r
    }
  }

  def getTfIdfRdd(lines_ids: RDD[(String, Long)], sc: SparkContext): RDD[(Long, Array[(Long, Int, Double)])] = {
    //    stopSet.take(10).foreach(println)
    val stopSet = getStopSetFromPath("cn_stopwords.txt")
    /*
      分词
     */

    val ids_tokenss_split = lines_ids
      .map(x => (x._2, tokenizer.tokens(x._1, stopSet)))
        println("id_tokenss_split")
        ids_tokenss_split.take(3).foreach(x => println(x._1 + " : " + x._2.mkString(",")))

    /*
      计算tf
     */
    val record_tf_rdd = ids_tokenss_split
      .map(tfMapWithSimhash)
    /*
      计算文档总数
     */
    val record_size = record_tf_rdd.count()
    logger.info("文档总数: " + record_size)

    /*
      计算文档平均长度、最大长度、最小长度
     */
    if(!debugMode.equalsIgnoreCase("fast")) {
      val lengthRdd = record_tf_rdd.map(x => x._2.length)
      val avg_len = lengthRdd.map(_ / record_size).sum()
      val max_len = lengthRdd.max()
      val min_len = lengthRdd.min()
      logger.info("平均长度: " + avg_len + " 最大长度: " + max_len + " 最小长度: " + min_len)
    }


    /*
      统计词频
     */
    val word_freq_rdd = record_tf_rdd
      .flatMap(x => x._2)
      .map(x => (x._1, 1))
      .reduceByKey(_+_)
      .map(x => (
        x._1,
        (x._2,
          Math.log((record_size + 1) / (x._2 + 1)))
      ))
      .sortBy(_._2._1,false)

//    println("freqMap")
//    word_freq_rdd.foreach(x => println(x._1 + ", " + x._2._1 + ", " + x._2._2))

    val word_size = word_freq_rdd.count()
    logger.info("word size " + word_size)
    Tools.saveRdd(sc, word_freq_rdd, rootPath + "/word_freq")
    /*
      计算idf
     */
    val word_freq_map = word_freq_rdd.filter(x => x._2._1 > 1)
      .collectAsMap()
    val word_freq_broad = sc.broadcast(word_freq_map)
    logger.info("after word size " + word_freq_map.size)
    if(debugMode.equalsIgnoreCase("debug")) {
      println("word_freq_broad.value.foreach(println)")
      word_freq_broad.value.take(10).foreach(println)
    }
    record_tf_rdd.map(x => idfMap(x, word_freq_broad.value, Math.log(record_size * 0.5)))
  }

  /*
  计算Tf值并拆分token list
  输出为单词字符串和其对应的tf值数组，用于整体统计，无编号
  */
  def tfMap(tokens: List[String]): Array[(String, Int)] = {
    val map = new mutable.HashMap[String, Int]()
    for (e <- tokens){
      if(map.contains(e)){
        map(e) += 1
      } else {
        map(e) = 1
      }
    }
    map.toArray
  }

  /*
   计算Tf值和并将单词转化为长整型化，最后拆分token list
   输出为记录编号+记录中每个单词对应的长整型和tf值构成的数组
   用于计算，有编号
  */
  def tfMapWithSimhash(id_tokens: (Long,List[String])): (Long, Array[(Long, Int)]) = {
    val map = new mutable.HashMap[Long, Int]()
    for (e <- id_tokens._2.toArray.map(x => algorithm.simhash.MurmurHash.hash64(x))){
      if(map.contains(e)){
        map(e) += 1
      } else {
        map(e) = 1
      }
    }
    (id_tokens._1, map.toArray)
  }

  /*
    计算idf，用于计算，有编号
    输出为(记录编号，[(单词编码，tf值，idf值)]
   */
  def idfMap(tfRecord: (Long, Array[(Long, Int)]), freqMap: collection.Map[Long, (Int, Double)], defaultIdf: Double): (Long, Array[(Long, Int, Double)]) = {
    (tfRecord._1, tfRecord._2
      .map(x => (
        x._1,
        x._2,
        if(freqMap.contains(x._1)) freqMap.get(x._1).get._1 else 1,
        if(freqMap.contains(x._1)) freqMap.get(x._1).get._2 else defaultIdf))
      .sortWith(
        (x, y) =>
          if(x._3 == y._3){
            x._1 < y._1
          } else {
            x._3 < y._3
          }
      )
      .map(x => (
        x._1,
        x._2,
        x._4)
      )
    )
  }


  /*
    计算idf，用于统计
    输入为[(单词，tf值)]
    输出为[(单词，tf值，idf值)]
   */
  def idfMap(tfRecord: Array[(String, Int)], freqMap: collection.Map[String, (Int, Double)]): Array[(String, Int, Double)] = {
    tfRecord
      .map(x => (
        x._1,
        x._2,
        freqMap.get(x._1).get._2)
      )
  }
}
