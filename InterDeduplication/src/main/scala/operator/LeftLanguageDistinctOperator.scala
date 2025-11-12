package operator

import com.alibaba.fastjson.JSON
import component._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.utils.Tools
import tokenizer.ITokenizer


/*
    language specific distinct
 */
class LeftLanguageDistinctOperator(rootPath: String, stop_words: String = "cn_stopwords.txt", language_mode: String = "all", langguage_ratio: Double = 0.001,
                                   tokenizer: ITokenizer, debug_mode: String = "work") extends Serializable {

  val logger = LoggerFactory.getLogger(this.getClass)

  def distinctData(input: String, spark: SparkSession, sc: SparkContext, json_field: String = "text", input_partitions: Int = 200, filter_length: Int = 0, filter_max_length: Int = Int.MaxValue, tokenize_length: Int = 0, file_mode: String, separator: String = "\n\n"): Unit = {

    val sourceParser = new SourceParser()
    val textStatistics = new TextStatistics(rootPath, stop_words, language_mode, tokenizer, debug_mode)
    val pattern = textStatistics.getLangRegex(language_mode)
    /* step0
       原始数据初步规整，编号和过滤无效文档
    */

    val lines_ids = if (!Tools.isHdfsPathExist(sc, rootPath + "/index_output")) {
      //生成带编号的文档集合

      val tempRdd = if (file_mode.equalsIgnoreCase("para")) {
        logger.info("read from para mode")
        sourceParser.getIndexedRddForPara(input, input_partitions, filter_length, filter_max_length, separator, spark, sc)
      } else if (file_mode.equalsIgnoreCase("file")) {
        logger.info("read from file mode")
        sourceParser.getIndexedRddForFile(input, input_partitions, filter_length, filter_max_length, spark, sc)
          .map(x => (x._1._2, x._2))
      } else {
        logger.info("read from line mode")
        sourceParser.getIndexedRddForLine(input, input_partitions, filter_length, filter_max_length, spark, sc)
      }
      if (debug_mode.equalsIgnoreCase("debug")) {
        tempRdd.map(x => x._2 + "\n" + x._1 + "\n").saveAsTextFile(rootPath + "/index_output")
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
    if (debug_mode.equalsIgnoreCase("debug")) {
      println("lines_ids.take(10).foreach(println)")
      lines_ids.take(10).foreach(println)
    }

    val id_lines = lines_ids.map(x => (x._2, x._1))
    val before_rdd = id_lines.map(x => {
      try {
        val jsonText = JSON.parseObject(x._2)
        var textStr = jsonText.getString(json_field)
        val mode = jsonText.getString("id")
        if (textStr == null) {
          logger.error("data:" + x + " --------" + "json_field:" + json_field + " not exist")
          textStr = ""
        }
        (textStr, (x._1, mode))
      } catch {
        case e: Exception => {
          logger.error("data:" + x + " --------" + e.toString)
          ("", (x._1, ""))
        }
      }

    }).filter(x => x._1.length > filter_length)
    val before_size = before_rdd.count()

    /*
      step 1: 自动计算停止词
     */
    val stopSet = if (language_mode.equals("auto")) {
      val auto_stop_size = if (langguage_ratio >= 1.0) {
        1.0;
      } else if (langguage_ratio < 0.00000000001) {
        0.0
      } else {
        langguage_ratio
      }
      val digitPattern = """\d+""".r
      val rdd1 = lines_ids.flatMap(x => tokenizer.tokens(x._1))
        .filter(x => digitPattern.findFirstIn(x) == None)
        .map(x => (x, 1))
        .reduceByKey(_ + _)
        .map(x => (x._2, x._1))
        .top(200)
      logger.info("top stop word count: " + before_size * auto_stop_size + " auto_stop_size: " + auto_stop_size)
      if (debug_mode.equalsIgnoreCase("debug")) {
        println("统计词频")
        rdd1.take(10).foreach(println)
      }
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
    /*
      step2 数据清洗，精确去重统计
     */

    var converted_rdd = before_rdd
      .groupByKey()
      .map(x => (x._1, x._2.toList))
    val after_size = converted_rdd.count()
    val duplicate_exact_size = before_size - after_size
    //    println("converted_rdd.take(10).foreach(println)")
    //    converted_rdd.take(10).foreach(println)
    //实际参与去重的是converted_rdd，duplicate_rdd_1只是所有重复的结果
    val exact_sample_ratio = if (duplicate_exact_size > 10000) {
      10000.0 / duplicate_exact_size
    } else {
      1.0
    }
    val duplicate_rdd_1 = converted_rdd.filter(x => x._2.length > 1).sample(false, exact_sample_ratio)
    //    println("duplicate_rdd_1.map(x => x._2.mkString(\",\")).take(3).foreach(println)")
    //    duplicate_rdd_1.map(x => x._2.mkString(",")).take(3).foreach(println)
    duplicate_rdd_1.map(x => x._2.map(x => x._1).mkString(","))
      .saveAsTextFile(rootPath + "/duplicate_output_1")
    val duplicate_detail_rdd_1 = duplicate_rdd_1.flatMap(x => x._2.map(id => (id._1, x._1)))
      .join(id_lines)
      .map(x => (x._2._1, x._1 + "\n" + x._2._2))
      .groupByKey()
      .map(x => x._2.mkString("\n") + "\n")

    /*
     去重
    */
    val converted_rdd_2 = converted_rdd
      .map(x => {
        val processed = pattern.replaceAllIn(x._1, " ")
        val words_length = tokenizer.tokens(processed).length
        val tokenized = if (words_length < tokenize_length) {
          tokenizer.tokens(processed)
        } else {
          tokenizer.tokens(processed, stopSet)
        }
        if (debug_mode.equalsIgnoreCase("debug")) {
          println("pattern:" + pattern)
          println("tokenized: " + tokenized.mkString(","))
        }
        val sorted = tokenized.distinct.sorted.mkString(",")
        (sorted, mapListToElement(x._2))     // 第一次去重
      })
      .groupByKey()
      .map(x => (x._1, x._2.toList))
    val after_size_2 = converted_rdd_2.count()
    val duplicate_fuzzy_size = after_size - after_size_2
    //    println("converted_rdd.take(10).foreach(println)")
    //    converted_rdd.take(10).foreach(println)
    //实际参与去重的是converted_rdd，duplicate_rdd_1只是所有重复的结果
    val fuzzy_sample_ratio = if (duplicate_fuzzy_size > 10000) {
      10000.0 / duplicate_fuzzy_size
    } else {
      1.0
    }
    val duplicate_rdd_2 = converted_rdd_2.filter(x => x._2.length > 1).sample(false, fuzzy_sample_ratio)
    //    println("duplicate_rdd_1.map(x => x._2.mkString(\",\")).take(3).foreach(println)")
    //    duplicate_rdd_1.map(x => x._2.mkString(",")).take(3).foreach(println)
    duplicate_rdd_2.map(x => x._2.map(x => x._1).mkString(","))
      .saveAsTextFile(rootPath + "/duplicate_output_2")

    val duplicate_detail_g_rdd_2 = duplicate_rdd_2.flatMap(x => x._2.map(id => (id._1, x._1)))
      .join(id_lines)
      .map(x => (x._2._1, x._1 + "\n" + x._2._2))
      .groupByKey()
    val duplicate_detail_rdd_2 = if (debug_mode.equalsIgnoreCase("debug")) {
      duplicate_detail_g_rdd_2
        .map(x => x._1 + ":----\n" + x._2.mkString("\n") + "\n")
    } else {
      duplicate_detail_g_rdd_2
        .map(x => x._2.mkString("\n") + "\n")
    }


    val reserved_rdd = converted_rdd_2.map(x => (mapListToElement(x._2)._1, None))    //第二次去重
      .join(id_lines)
      .map(x => x._2._2)
      .cache()

    val eval_reserved_rdd = reserved_rdd.filter(x => {
        val mode = JSON.parseObject(x).getString("id")
        mode.startsWith("032-1-")
    })
    Tools.saveRdd(sc, eval_reserved_rdd, rootPath + "/eval_output")

//    Tools.saveRdd(sc, reserved_rdd, rootPath + "/reserved_output")
    val reserved_size = reserved_rdd.count()




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
    duplicate_detail_rdd_2.saveAsTextFile(rootPath + "/duplicate_detail_output_2")
  }


  def mapListToElement(list: List[(Long, String)]): (Long, String) = {
      if (list.size <= 1) {
        return list.apply(0)
      }
      for ((id, mode) <- list) {
        if (!mode.startsWith("032-1-")){
          return (id, mode)
        }
    }
    list.apply(0)
  }

}

