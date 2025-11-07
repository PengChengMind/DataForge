package operator

import com.alibaba.fastjson.JSON
import component._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.utils.Tools
import tokenizer.ITokenizer

import java.nio.file.Paths
import scala.util.matching.Regex


/*
    language specific distinct
 */
class LanguageCompareDistinctOperator(rootPath: String, stop_words: String = "cn_stopwords.txt", language_mode: String = "all", langguage_ratio: Double = 0.001,
                                      sample_count: Int, tokenizer: ITokenizer, debug_mode: String = "work") extends Serializable {

  val logger = LoggerFactory.getLogger(this.getClass)


  def getRddFromPath(path: String, file_mode: String, input_partitions: Int, filter_length: Int, filter_max_length: Int, separator: String, spark: SparkSession, sourceParser: SourceParser, sc: SparkContext): RDD[(String, Long)] = {

    val fileName = Paths.get(path).getFileName.toString
    val indexPath = rootPath + "/" + fileName + "_index_output"
    if (!Tools.isHdfsPathExist(sc, indexPath)) {
      //生成带编号的文档集合
      val tempRdd = if (file_mode.equalsIgnoreCase("para")) {
        logger.info("read from para mode")
        sourceParser.getIndexedRddForPara(path, input_partitions, filter_length, filter_max_length, separator, spark, sc)
      } else if (file_mode.equalsIgnoreCase("file")) {
        logger.info("read from file mode")
        sourceParser.getIndexedRddForFile(path, input_partitions, filter_length, filter_max_length, spark, sc)
          .map(x => (x._1._2, x._2))
      } else {
        logger.info("read from line mode")
        sourceParser.getIndexedRddForLine(path, input_partitions, filter_length, filter_max_length, spark, sc)
      }
      if (debug_mode.equalsIgnoreCase("debug")) {
        tempRdd.map(x => x._2 + "\n" + x._1 + "\n").saveAsTextFile(rootPath + "/index_output")
      }
      logger.info(rootPath + "/index_output path dose not exist, created new index_output")
      tempRdd
    } else {
      // 直接从文件读取生成过的编号文件
      logger.info("directly read index_path from " + indexPath)
      val lines_ids_pattern = """^\((.*),(\d+)\)$""".r
      sc.textFile(indexPath)
        .map(x => lines_ids_pattern.findAllMatchIn(x).next())
        .map(x => (x.group(1), x.group(2).toLong))
    }
  }

  def getStopSetFromRdd(line_ids: RDD[(String, Long)], stop_size: Int, left_before_size: Long, textStatistics: TextStatistics, spark: SparkSession): Set[String] = {
    if (stop_size > 0) {
        val auto_stop_size = if (langguage_ratio >= 1.0) {
          1.0;
        } else if (langguage_ratio < 0.00000000001) {
          0.0
        } else {
          langguage_ratio
        }
        val digitPattern = """\d+""".r
        val rdd1 = line_ids.flatMap(x => tokenizer.tokens(x._1))
          .filter(x => digitPattern.findFirstIn(x) == None)
          .map(x => (x, 1))
          .reduceByKey(_ + _)
          .map(x => (x._2, x._1))
          .top(stop_size)
        logger.info("top stop word count: " + left_before_size * auto_stop_size + " auto_stop_size: " + auto_stop_size)
        if (debug_mode.equalsIgnoreCase("debug")) {
          println("统计词频")
          rdd1.take(10).foreach(println)
        }

        // 将数组转换为 RDD
        val auto_stop_rdd = spark.sparkContext.parallelize(rdd1)

        // 保存数组到文件
        auto_stop_rdd.map(_.toString).saveAsTextFile(rootPath + "/stop_words")
  //      println("stop_size:" + stop_size + " rdd1:" + rdd1.toString)

        val freq_set = rdd1
          .map(x => x._2)
          .toSet
        if (debug_mode.equalsIgnoreCase("debug")) {
          println("生成词表：")
          freq_set.foreach(println)
        }
        textStatistics.getStopSet().++(freq_set)
      } else {
        textStatistics.getStopSet()
      }
  }

  def distinctData(inputL: String, inputR: String, spark: SparkSession, sc: SparkContext, json_field: String = "text", input_partitions: Int = 200, filter_length: Int = 0, filter_max_length: Int = Int.MaxValue, tokenize_length: Int = 0, stop_size: Int = 0, file_mode: String, separator: String = "\n\n"): Unit = {

    val sourceParser = new SourceParser()
    val textStatistics = new TextStatistics(rootPath, stop_words, language_mode, tokenizer, debug_mode)
    val pattern = textStatistics.getLangRegex(language_mode)
    /* step0
       原始数据初步规整，编号和过滤无效文档
    */

    val left_lines_ids = getRddFromPath(inputL, file_mode, input_partitions, filter_length, filter_max_length, separator, spark, sourceParser, sc)
    if (debug_mode.equalsIgnoreCase("debug")) {
      println("lines_ids.take(10).foreach(println)")
      left_lines_ids.take(10).foreach(println)
    }
    val right_lines_ids = getRddFromPath(inputR, file_mode, input_partitions, filter_length, filter_max_length, separator, spark, sourceParser, sc)

    val left_id_lines = left_lines_ids.map(x => (x._2, x._1))
    val right_id_lines = right_lines_ids.map(x => (x._2, x._1))

    val left_before_rdd = if (file_mode.equalsIgnoreCase("json")) {
       left_id_lines.map(x => {
        try {
          var textStr = JSON.parseObject(x._2).getString(json_field)
          if (textStr == null) {
            logger.error("data:" + x + " --------" + "json_field:" + json_field + " not exist")
            textStr = ""
          }
          (textStr, x._1)
        } catch {
          case e: Exception => {
            logger.error("data:" + x + " --------" + e.toString)
            ("", x._1)
          }
        }

      }).filter(x => (x._1.length > filter_length && x._1.length <= filter_max_length))
    } else left_lines_ids
    val left_before_size = left_before_rdd.count()
    
    val right_before_rdd = if (file_mode.equalsIgnoreCase("json")) {
       right_id_lines.map(x => {
        try {
          var textStr = JSON.parseObject(x._2).getString(json_field)
          if (textStr == null) {
            logger.error("data:" + x + " --------" + "json_field:" + json_field + " not exist")
            textStr = ""
          }
          (textStr, x._1)
        } catch {
          case e: Exception => {
            logger.error("data:" + x + " --------" + e.toString)
            ("", x._1)
          }
        }

      }).filter(x => (x._1.length > filter_length && x._1.length <= filter_max_length))
    } else right_lines_ids
    val right_before_size = right_before_rdd.count()
    /*
      step 1: 自动计算停止词
     */
//    val leftStopSet = getStopSetFromRdd(left_lines_ids, stop_size, left_before_size, textStatistics, spark)
    val rightStopSet = getStopSetFromRdd(right_lines_ids, stop_size, right_before_size, textStatistics, spark)
    /*
      step2 数据清洗，精确去重统计，由于需要统计精确比例，所以还是分两步，但是先模糊，后筛选精确，这样可以只进行一次join
     */


    var left_converted_rdd = left_before_rdd.map(x => convertRecordToSet(x, pattern, tokenizer, tokenize_length, rightStopSet, debug_mode))
    val right_converted_rdd = right_before_rdd.map(x => convertRecordToSet(x, pattern, tokenizer, tokenize_length, rightStopSet, debug_mode))


    val joinedRDD = left_converted_rdd.leftOuterJoin(right_converted_rdd)

    var left_reserved_rdd = joinedRDD.filter(x => x._2._2.isEmpty)
      .map(x => (x._2._1._2, x._2._1._1))

    val duplicate_rdd = joinedRDD.filter(x => x._2._2.isDefined)

    println("left_converted_rdd count:" + left_converted_rdd.count() + " right_converted_rdd:" + right_converted_rdd.count()  + " right_converted_rdd count:" + right_converted_rdd.count() +   "joinedRDD count:" + joinedRDD.count() + " left count:" + left_reserved_rdd.count() + " duplicate_rdd:" + duplicate_rdd.count())

    val exact_duplicate_rdd = duplicate_rdd.filter(x => x._2._1._1.equals(x._2._2.get._1)).map(x => (x._2._1, x._2._2.get))

    val fuzzy_duplciate_rdd = duplicate_rdd.filter(x => !x._2._1._1.equals(x._2._2.get._1)).map(x => (x._2._1, x._2._2.get))


    val left_after_size = left_reserved_rdd.count()
    val duplicate_exact_size = exact_duplicate_rdd.count()
    //    println("converted_rdd.take(10).foreach(println)")
    //    converted_rdd.take(10).foreach(println)
    //实际参与去重的是converted_rdd，duplicate_rdd_1只是所有重复的结果
    val exact_sample_ratio = if (sample_count > 0 && duplicate_exact_size > sample_count) {
      sample_count * 1.0 / duplicate_exact_size
    } else {
      1.0
    }
    logger.info("exact_sample_ratio = " + exact_sample_ratio + " duplicate_exact_size = " + duplicate_exact_size + " sample_count = " + sample_count)
    var duplicate_rdd_1 = exact_duplicate_rdd
    if (sample_count > 0) {
      duplicate_rdd_1 = duplicate_rdd_1.sample(false, exact_sample_ratio)
    }
    //    println("duplicate_rdd_1.map(x => x._2.mkString(\",\")).take(3).foreach(println)")
    //    duplicate_rdd_1.map(x => x._2.mkString(",")).take(3).foreach(println)
    duplicate_rdd_1.map(x => x._1._2 + "," + x._2._2)
      .saveAsTextFile(rootPath + "/duplicate_output_1")
    val duplicate_detail_rdd_1 = duplicate_rdd_1.map(x => x._1._1 + "\n" + x._2._1 + "\n")



    val duplicate_fuzzy_size = fuzzy_duplciate_rdd.count()
    //    println("converted_rdd.thake(10).foreach(println)")
    //    converted_rdd.take(10).foreach(println)
    //实际参与去重的是converted_rdd，duplicate_rdd_1只是所有重复的结果
    val fuzzy_sample_ratio = if (sample_count > 0 && duplicate_fuzzy_size > sample_count) {
      sample_count * 1.0 / duplicate_fuzzy_size
    } else {
      1.0
    }
    logger.info("fuzzy_sample_ratio = " + fuzzy_sample_ratio + " sample_count = " + sample_count + " duplicate_fuzzy_size = " + duplicate_fuzzy_size)
    var duplicate_rdd_2 = fuzzy_duplciate_rdd
    if (sample_count > 0) {
      duplicate_rdd_2 = duplicate_rdd_2.sample(false, fuzzy_sample_ratio)
    }
    //    println("duplicate_rdd_1.map(x => x._2.mkString(\",\")).take(3).foreach(println)")
    //    duplicate_rdd_1.map(x => x._2.mkString(",")).take(3).foreach(println)
    duplicate_rdd_2.map(x => x._1._2 + "," + x._2._2)
      .saveAsTextFile(rootPath + "/duplicate_output_2")

    val duplicate_detail_rdd_2 = duplicate_rdd_2.map(x => x._1._1 + "\n" + x._2._1 + "\n")


    val reserved_rdd = left_reserved_rdd.join(left_id_lines)
      .map(x => x._2._2)
    Tools.saveRdd(sc, reserved_rdd, rootPath + "/reserved_output")
    val reserved_size = reserved_rdd.count()

    println("文档总数: " + left_before_size)
    println("全局去重后文档总数: " + reserved_size)
    println(f"整体重复率：${(duplicate_exact_size + duplicate_fuzzy_size) * 1.0 / left_before_size * 100}%.2f%%")
    println(f"精确重复率: ${duplicate_exact_size * 1.0 / left_before_size * 100}%.2f%%")
    println(f"模糊重复率：${duplicate_fuzzy_size * 1.0 / left_before_size * 100}%.2f%%")
    println("精确重复文档数: " + duplicate_exact_size)
    println("精确去重后文档总数: " + (left_before_size - duplicate_exact_size))
    println("模糊重复文档数：" + duplicate_fuzzy_size)
    println("模糊去重后文档总数：" + reserved_size)
    println("精确重复样例数据：")
    duplicate_detail_rdd_1.take(3).foreach(println)
    duplicate_detail_rdd_1.saveAsTextFile(rootPath + "/duplicate_detail_output_1")
    println("模糊重复样例数据")
    duplicate_detail_rdd_2.take(3).foreach(println)
    duplicate_detail_rdd_2.saveAsTextFile(rootPath + "/duplicate_detail_output_2")
  }


  def convertRecordToSet(x: (String, Long), pattern: Regex, tokenizer: ITokenizer, tokenize_length: Int, leftStopSet: Set[String], debug_mode: String): (String, (String, Long)) = {
    val processed = pattern.replaceAllIn(x._1, " ").toLowerCase()
    val words_length = tokenizer.tokens(processed).length
    val tokenized = if (words_length < tokenize_length) {
      tokenizer.tokens(processed)
    } else {
      tokenizer.tokens(processed, leftStopSet)
    }

    if (debug_mode.equalsIgnoreCase("debug")) {
      println("pattern: " + pattern)
      println("tokenized: " + tokenized.mkString(","))
    }

    val sorted = tokenized.distinct.sorted.mkString(",")
    (sorted, x)
  }

}

