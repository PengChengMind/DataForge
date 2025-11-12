package tool.distinct

import component.DynamicParameter
import operator.{LanguageDistinctOperator, SimhashPrefixIdfDistinctOperator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import org.utils.Tools
import tokenizer.{DefinedTokenizer, EnTokenizer, JiebaTokenizer, SepTokenizer}

import scala.sys.process._

object LanguageExactDuplicate {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {


    // 参数解析
    val dynamicParameter = new DynamicParameter
    val options = dynamicParameter.parseDistinct(Map(), args.toList)
    var input = options.getOrElse('input, "input/*mini.txt")
    val output = options.getOrElse('output, "output")
    val sample_rate = options.getOrElse('sample_rate, 1.0).toString.toDouble
    val filter_length = options.getOrElse('filter_length, 0).toString.toInt
    val filter_max_length = options.getOrElse('filter_max_length, 100000).toString.toInt
    var tokenize_length = options.getOrElse('tokenize_length, 8).toString.toInt
    var input_partitions = options.getOrElse('input_partitions, 200).toString.toInt
    var file_mode = options.getOrElse('file_mode, "line")
    val json_field = options.getOrElse('json_field, "text")
    var language_mode = options.getOrElse('language_mode, "common")
    var language_ratio = options.getOrElse('language_ratio, 0.001).toString.toDouble
    var sample_count = options.getOrElse('sample_count, 10000).toString.toInt
    var stop_size = options.getOrElse('stop_size, 200).toString.toInt
    var debug_mode = options.getOrElse('debug_mode, "")
    var tokenizer_mode = options.getOrElse('tokenizer_mode, "jieba")
    val stop_words = options.getOrElse('stop_words, "cn_stopwords.txt")
    val separator = options.getOrElse('separator, "\n\n")
    val appName = "LanguageExactDuplicate" + "_" + language_mode + "_" + input.replaceAll("txt", "").substring(Math.max(0, input.replaceAll("txt", "").length - 15))
    val conf = new SparkConf()
      .setAppName(appName)

    if (args.length == 0) {
      conf.setMaster("local")
        .set("spark.sql.shuffle.partitions", "4")
//      input = "/Users/stardust/Downloads/c4-validation.00000-of-00001.json"
//      input = "/Users/stardust/code/StardustScalaProjects/SparkScalaProjects/input/test_distinct.txt"
//      input = "/Users/stardust/Downloads/eng_test.txt"
      input = "/Users/stardust/Downloads/test_cn.txt"
      file_mode = "json"
      input_partitions = 1
      language_ratio = 0.001
      debug_mode = "debug"
      tokenize_length = 8
      sample_count = 0
      tokenizer_mode = "jieba"
      language_mode = "auto"
      val cmd = "rm -rf " + output
      cmd.!!
    }


    if (language_mode.equalsIgnoreCase("auto") && stop_size == 200) {
      stop_size = 40000
    }

    println(appName + " version 1.0 input = " + input + " output = " + output +
      " threshold_distance = " + " language_mode: " + language_mode + " language_ratio: " + language_ratio + " tokenizer_mode:" + tokenizer_mode + " file_mode:" + file_mode + " languageMode:" + language_mode + " filter_length:" + filter_length + " filter_max_length:" + filter_max_length + " stop_size:" + stop_size + " sample_rate:" + sample_rate)
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.getOrCreate()
    var tokenizer = if (tokenizer_mode.equalsIgnoreCase("jieba")) {
      new JiebaTokenizer
    } else if (tokenizer_mode.equals("defined")) {
      new DefinedTokenizer(separator)
    } else {
      new EnTokenizer
    }

    if (language_mode.equalsIgnoreCase("auto")){
      tokenizer = new SepTokenizer
    }



    if(Tools.isHdfsPathExist(sc, output)){
      println("当前文件处理结果已存在，请先确认并清理结果后重试: " + output)
      System.exit(1)
    }

    //     val  sipDistinctOperator = new SIPDistinctOperator(output)
    val distinctOperator = new LanguageDistinctOperator(output, stop_words, language_mode, language_ratio, sample_count, tokenizer, debug_mode)
    //    sipDistinctOperator.distinctForLine(input, spark, sc)
    distinctOperator.distinctData(input = input, spark = spark, sc = sc, json_field = json_field, input_partitions = input_partitions, filter_length = filter_length, filter_max_length = filter_max_length, tokenize_length = tokenize_length, stop_size = stop_size, file_mode = file_mode, separator = separator, sample_rate = sample_rate)
  }

}
