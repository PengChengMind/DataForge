package tool.distinct

import component.DynamicParameter
import operator.SimhashPrefixIdfDistinctOperator
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import org.utils.Tools
import tokenizer.{DefinedTokenizer, EnTokenizer, JiebaTokenizer, SepTokenizer}

import scala.sys.process._

object SimhashPrefixIdfDuplicate {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {


    // 参数解析
    val dynamicParameter = new DynamicParameter
    val options = dynamicParameter.parseDistinct(Map(), args.toList)
    var input = options.getOrElse('input, "input/*mini.txt")
    val output = options.getOrElse('output, "output")
    var jaccard_distance = options.getOrElse('jaccard_distance, 0.1).toString.toDouble
    var haming_distance = options.getOrElse('haming_distance, 3).toString.toInt
    val filter_length = options.getOrElse('filter_length, 0).toString.toInt
    val filter_max_length = options.getOrElse('filter_max_length, 100000).toString.toInt
    var input_partitions = options.getOrElse('input_partitions, 200).toString.toInt
    var sql_partitions = options.getOrElse('sql_partitions, 200).toString.toInt
    var shuffle_partitions = options.getOrElse('shuffle_partitions, 200).toString.toInt
    var file_mode = options.getOrElse('file_mode, "line")
    val json_field = options.getOrElse('json_field, "text")
    var language_mode = options.getOrElse('language_mode, "common")
    val match_mode = options.getOrElse('match_mode, "fuzzy")
    var debug_mode = options.getOrElse('debug_mode, "")
    var tokenizer_mode = options.getOrElse('tokenizer_mode, "jieba")
    val stop_words = options.getOrElse('stop_words, "cn_stopwords.txt")
    val separator = options.getOrElse('separator, "\n\n")
    val appName = "SimhashWithIdf" + "_" + jaccard_distance.formatted("%.1f") + "_" + haming_distance + "_" + language_mode + "_" + input.replaceAll("txt", "").substring(Math.max(0, input.replaceAll("txt", "").length - 15))
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.sql.shuffle.partitions", sql_partitions.toString)

    if (args.length == 0) {
      conf.setMaster("local")
        .set("spark.sql.shuffle.partitions", "1")
      input = "/Users/stardust/Downloads/cn_test.txt"
      file_mode = "json"
      input_partitions = 1
      sql_partitions = 1
      debug_mode = "debug"
      jaccard_distance = 0.9
      haming_distance = 20
      language_mode = "cn"
      tokenizer_mode = "jieba"
      shuffle_partitions = 1
//      debug_mode = "debug"
      val cmd = "rm -rf " + output
      cmd.!!
    }

    logger.info(appName + " version 1.0 input = " + input + " output = " + output +
      " threshold_distance = " + jaccard_distance + " hamming_distance: " +
      haming_distance + " language_mode: " + language_mode + " match_mode: " +
      match_mode + " tokenizer_mode:" + tokenizer_mode + " sql_partitions:" + sql_partitions + " input_partitions:" + input_partitions + " shuffle_partitions:" + shuffle_partitions)
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.getOrCreate()
    var tokenizer = if (tokenizer_mode.equalsIgnoreCase("jieba")) {
      new JiebaTokenizer
    } else if (tokenizer_mode.equalsIgnoreCase("sep")) {
      new SepTokenizer
    } else if (tokenizer_mode.equals("defined")) {
      new DefinedTokenizer(separator)
    } else {
      new EnTokenizer
    }

    if (language_mode.equalsIgnoreCase("auto")) {
      tokenizer = new SepTokenizer
    }

    if(Tools.isHdfsPathExist(sc, output)){
      println("当前文件处理结果已存在，请先确认并清理结果后重试: " + output)
      System.exit(1)
    }
    //     val  sipDistinctOperator = new SIPDistinctOperator(output)
    val distinctOperator = new SimhashPrefixIdfDistinctOperator(output, stop_words, language_mode, tokenizer, haming_distance, jaccard_distance, debug_mode)
    //    sipDistinctOperator.distinctForLine(input, spark, sc)
    if (file_mode.equalsIgnoreCase("para")) {
      distinctOperator.distinctForLine(input = input, spark = spark, sc = sc, input_partitions = input_partitions, filter_length = filter_length, shuffle_partitions = shuffle_partitions, match_mode = match_mode, file_mode = file_mode, separator = separator)
    } else if (file_mode.equalsIgnoreCase("file")) {
      distinctOperator.distinctForFile(input = input, spark = spark, sc = sc, input_partitions = input_partitions, filter_length = filter_length, match_mode = match_mode)
    } else {
      distinctOperator.distinctForLine(input = input, spark = spark, sc = sc, jsonField = json_field, input_partitions = input_partitions, filter_length = filter_length, filter_max_length = filter_max_length, shuffle_partitions = shuffle_partitions, match_mode = match_mode, file_mode = file_mode)
    }

  }


}
