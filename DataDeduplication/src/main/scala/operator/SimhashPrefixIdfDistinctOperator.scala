package operator

import algorithm.simhash.Simhash
import com.alibaba.fastjson.JSON
import component._
import org.apache.commons.text.similarity.JaccardDistance
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import org.utils.Tools
import tokenizer.ITokenizer

import scala.sys.process._


/*
  SimhashIdfPrefix=SIP
  rootPath: 输出根目录
  stop_words：停止词文件
  language_mode：语言模式，all=[中文、英文、数字、下划线]，cn=[中文]，en=[英文]，其它字符将被过滤
  tokenizer_mode：分词模式，jieba=jieba分词，sep=分隔符分词,包括英文逗号和空白符
  haming_distance：哈明距离
  jaccard_distance：jaccard距离
  debug_mode：开发模式，为true时会打印一些调试信息
 */
class SimhashPrefixIdfDistinctOperator(rootPath: String, stop_words: String = "cn_stopwords.txt", language_mode: String = "all",
                                       tokenizer: ITokenizer, haming_distance: Int = 3, jaccard_distance: Double = 0.1,
                                       debug_mode: String = "work") extends Serializable {

  val logger = LoggerFactory.getLogger(this.getClass)


  def distinctForLine(input: String, spark: SparkSession, sc: SparkContext, jsonField: String = "text", input_partitions: Int = 200, filter_length: Int = 0, filter_max_length: Int = 100000, shuffle_partitions: Int = 200, match_mode: String = "fuzzy", file_mode: String = "line", separator: String = "\n\n"): Unit = {

    val sourceParser = new SourceParser()
    val simHash = new Simhash()
    val textStatistics = new TextStatistics(rootPath, stop_words, language_mode, tokenizer, debug_mode)
    val pattern = textStatistics.getLangRegex()
    /* step1
       原始数据初步规整，编号和过滤无效文档
    */

    var lines_ids = if (!Tools.isHdfsPathExist(sc, rootPath + "/index_output")) {
      //生成带编号的文档集合

      val tempRdd = if(file_mode.equalsIgnoreCase("para")){
        logger.info("read from para mode")
        val para_rdd = sourceParser.getIndexedRddForPara(input, input_partitions, filter_length, filter_max_length, separator, spark, sc)
        if(!debug_mode.equalsIgnoreCase("fast")){
          para_rdd.map(x => x._2 + "\n" + x._1 + "\n")
            .saveAsTextFile(rootPath + "/index_output")
        }
        para_rdd
      } else if(file_mode.equalsIgnoreCase("json")) {
        logger.info("read from json mode, jsonField:" + jsonField)
        val para_rdd = sourceParser.getIndexedRddForLine(input, input_partitions, filter_length, filter_max_length, spark, sc)
          .map(x => {
            try {
              (JSON.parseObject(x._1).getString(jsonField), x._2)
            } catch {
              case e: Exception => {
                logger.error("data:" + x + " --------" + e.toString)
                ("", x._2)
              }
            }

          })
          .filter(x => x._1.length > filter_length)
        if(!debug_mode.equalsIgnoreCase("fast")) {
          para_rdd.saveAsTextFile(rootPath + "/index_output")
        }
        para_rdd
      } else {
        logger.info("read from line mode")
        val para_rdd = sourceParser.getIndexedRddForLine(input, input_partitions, filter_length, filter_max_length, spark, sc)
        if(!debug_mode.equalsIgnoreCase("fast")) {
          para_rdd.saveAsTextFile(rootPath + "/index_output")
        }
        para_rdd
      }
      logger.info(rootPath + "/index_output path dose not exist, created new index_output")
      tempRdd
    } else {
      // 直接从文件读取生成过的编号文件
      logger.info("directly read index_path from " + rootPath + "/index_output")
      val lines_ids_pattern = """^\((.*),(\d+)\)$""".r
      sc.textFile(rootPath + "/index_output")
        .map(x => lines_ids_pattern.findAllMatchIn(x).next())
        .map(x => (x.group(1), x.group(2).toLong))
    }
    if(debug_mode.equalsIgnoreCase("debug")){
      println("lines_ids.take(10).foreach(println)")
      lines_ids.take(30).foreach(println)
    }
    val id_lines = lines_ids.map(x => (x._2, x._1))

    /*
      step2 数据清洗，精确去重统计
     */

    val before_rdd = lines_ids
    val before_size = before_rdd.count()
    val converted_rdd =  before_rdd.groupByKey()
      .map(x => (x._1, x._2.toList))
    val after_size = converted_rdd.count()
    val duplicate_exact_size = before_size - after_size
//    println("converted_rdd.take(10).foreach(println)")
//    converted_rdd.take(10).foreach(println)
    //实际参与去重的是converted_rdd，duplicate_rdd_1只是所有重复的结果
    val duplicate_rdd_1 = converted_rdd.filter(x => x._2.length > 1)
//    println("duplicate_rdd_1.map(x => x._2.mkString(\",\")).take(3).foreach(println)")
//    duplicate_rdd_1.map(x => x._2.mkString(",")).take(3).foreach(println)
    duplicate_rdd_1.map(x => x._2.mkString(","))
      .saveAsTextFile(rootPath + "/duplicate_output_1")
    val duplicate_detail_rdd_1 = duplicate_rdd_1.flatMap(x => x._2.map(id => (id, x._1)))
      .join(id_lines)
      .map(x => (x._2._1, x._1 + "\n" + x._2._2))
      .groupByKey()
      .map(x => x._2.mkString("\n") + "\n")
    /* step3
       数据清洗，去停止词、分词、词频统计、计算TFIDF
     */

    lines_ids = converted_rdd.map(x => (x._1, x._2.apply(0)))
      .persist(StorageLevel.MEMORY_AND_DISK)
    val converted_lines =  lines_ids.map(x => (pattern.replaceAllIn(x._1, " "), x._2))
    if(debug_mode.equalsIgnoreCase("debug")){
      println("converted_lines.take(10).foreach(println)")
      converted_lines.take(10).foreach(println)
    }
    val record_tfidf_rdd = textStatistics.getTfIdfRdd(converted_lines, sc)
    if(debug_mode.equalsIgnoreCase("debug")){
      println("record_tfidf_rdd.take(10).foreach(println)")
      record_tfidf_rdd.take(10).sortBy(_._1).foreach(x => println(x._1, x._2.mkString(",")))
    }

    /*
      前缀剪枝
     */
    val textSimilarity = new TextSimilarity(haming_distance, jaccard_distance, debug_mode)
    val flat_prefix_rdd = record_tfidf_rdd
      .flatMap(textSimilarity.prefixFlatMapWithSimHash(_, simHash))


    if(debug_mode.equalsIgnoreCase("debug")){
      println("flat_prefix_rdd.take(10).foreach(println)")
      flat_prefix_rdd.take(10).foreach(println)
      println("flat_prefix_rdd count=" + flat_prefix_rdd.count())
    }

    val group_rdd = flat_prefix_rdd
      .groupByKey(shuffle_partitions)

//    println("group_rdd.take(3).foreach(println)")
//    group_rdd.take(3).foreach(println)
    /*
      相似度验证
     */
    val sim_rdd = group_rdd
      .flatMap(textSimilarity.groupSimhashFlatMapWithWeight(_, match_mode))
      .distinct()
      .sortByKey()

//      println("模糊相似对数量：" + sim_rdd.count())
//    println("sim_rdd.take(20).foreach(println)")
//    sim_rdd.take(20).foreach(println)

    sim_rdd.saveAsTextFile(rootPath + "/sim_output")

    /*
      相似对映射真实数据
     */


    if(debug_mode.equalsIgnoreCase("debug")) {
      val sim_detail_rdd = sim_rdd
        .join(id_lines)
        .map(x => (x._2._1, (x._1, x._2._2)))
        .join(id_lines)
        .map(x => (x._2._1._1, x._2._1._2, x._1, x._2._2))

      val sim_detail_rdd_string = sim_detail_rdd.map(x => x._1 + "\n" + x._2 + "\n" + x._3 + "\n" + x._4 + "\n")
      val sim_detail_fuzzy_rdd_string = sim_detail_rdd
        .filter(x => !x._2.equalsIgnoreCase(x._4))
        .map(x => x._1 + "\n" + x._2 + "\n" + x._3 + "\n" + x._4 + "\n")
//      println("size: " + id_lines.count() + " : " + sim_detail_rdd.count() + " : " + sim_rdd.count())
//      println("sim_detail_rdd.take(1).foreach(println)")
//      sim_detail_rdd_string.take(1).foreach(println)
      sim_detail_rdd_string.saveAsTextFile(rootPath + "/sim_detail_output")
      sim_detail_fuzzy_rdd_string.saveAsTextFile(rootPath + "/sim_detail_fuzzy_output")
    }

    /*
      去重
     */
    val textDistinct = new TextDistinct(rootPath, debug_mode)
    val (duplicate_fuzzy_size,reserved_size, duplicate_detail_rdd_2) = textDistinct.saveDeduplicateLineResult(sim_rdd, lines_ids, spark, sc, file_mode)
    println("文档总数: " + before_size)
    println("全局去重后文档总数: " + reserved_size)
    println(f"整体重复率：${(duplicate_exact_size + duplicate_fuzzy_size) * 1.0 / before_size * 100}%.2f%%")
    println(f"精确重复率: ${duplicate_exact_size * 1.0 / before_size * 100}%.2f%%")
    println(f"模糊重复率：${duplicate_fuzzy_size * 1.0 / before_size * 100}%.2f%%")
    println("精确重复文档数: " + duplicate_exact_size)
    println("精确去重后文档总数: " + after_size)
    println("模糊重复文档数：" + duplicate_fuzzy_size)
    println("模糊去重后文档总数：" + reserved_size)
    println("精确重复样例数据：")
    duplicate_detail_rdd_1.take(3).foreach(println)
    duplicate_detail_rdd_1.saveAsTextFile(rootPath + "/duplicate_detail_output_1")
    println("模糊重复样例数据")
    duplicate_detail_rdd_2.take(3).foreach(println)
  }


  def distinctForFile(input: String, spark: SparkSession, sc: SparkContext, input_partitions: Int = 200, filter_length: Int = 0, filter_max_length: Int = 100000,shuffle_partitions: Int = 200, match_mode: String = "fuzzy"): Unit = {

    val sourceParser = new SourceParser()
    val simHash = new Simhash()
    /* step1
       原始数据初步规整，编号和过滤无效文档
    */

    val file_lines_ids = sourceParser.getIndexedRddForFile(input, input_partitions, filter_length, filter_max_length, spark, sc)
    val lines_ids = file_lines_ids.map(x => (x._1._2, x._2))
    val id_files = file_lines_ids.map(x => (x._2, x._1._1))

    /* step2
      数据清洗、去停止词、分词、词频统计、计算TF-IDF
     */

    val textStatistics = new TextStatistics(rootPath, stop_words, language_mode, tokenizer, debug_mode)
    val record_tfidf_rdd = textStatistics.getTfIdfRdd(lines_ids, sc)
//    println("record_tfidf_rdd.take(3).foreach(println)")
//    record_tfidf_rdd.take(3).foreach(x => println(x._1, x._2.mkString(",")))

    /*
      前缀剪枝
     */
    val textSimilarity = new TextSimilarity(haming_distance, jaccard_distance, debug_mode)
    val flat_prefix_rdd = record_tfidf_rdd
      .flatMap(textSimilarity.prefixFlatMapWithSimHash(_, simHash))


//    println("flat_prefix_rdd.take(3).foreach(println)")
//    flat_prefix_rdd.take(3).foreach(println)
    println("flat_prefix_rdd count=" + flat_prefix_rdd.count())

    val group_rdd = flat_prefix_rdd
      .groupByKey(shuffle_partitions)

//    println("group_rdd.take(3).foreach(println)")
//    group_rdd.take(3).foreach(println)

    /*
      相似度验证
     */
    val sim_rdd = group_rdd
      .flatMap(textSimilarity.groupSimhashFlatMapWithWeight(_, match_mode))
      .distinct()
      .sortByKey()

//    println("distinct_rdd.take(20).foreach(println)")
    println("distinct_rdd count = " + sim_rdd.count())
//    sim_rdd.take(20).foreach(println)

    sim_rdd.saveAsTextFile(rootPath + "/sim_output")

    /*
     相似对映射真实数据
    */
    val sim_detail_rdd = sim_rdd
      .join(id_files)
      .map(x => (x._2._1, (x._1, x._2._2)))
      .join(id_files)
      .map(x => (x._2._1._1, x._2._1._2, x._1, x._2._2))

//    println("sim_detail_rdd.take(3).foreach(println)")
//    sim_detail_rdd.take(3).foreach(println)
    sim_detail_rdd.saveAsTextFile(rootPath + "/sim_detail_output")

    /*
      去重
     */
    val textDistinct = new TextDistinct(rootPath, debug_mode)
    textDistinct.saveDeduplicateFileResult(sim_rdd, lines_ids, id_files, spark, sc)
  }


}
