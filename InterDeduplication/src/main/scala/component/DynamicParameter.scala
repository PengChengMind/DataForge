package component

class DynamicParameter {


  val usage =
    """
    Usage: spark-submit --master=yarn xx.jar [-h] [-p] <inputCsvPath> [-o] <outputLogPath> [-f] <testFuncName>
  """

  type OptionMap = Map[Symbol, String]

  def parseMl(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "--num_hashFeature" :: value :: tail =>
        parseMl(map ++ Map('num_hashFeature -> value), tail)
      case "--filter_length" :: value :: tail =>
        parseMl(map ++ Map('filter_length -> value), tail)
      case "--input_partitions" :: value :: tail =>
        parseMl(map ++ Map('input_partitions -> value), tail)
      case "--positive_partitions" :: value :: tail =>
        parseMl(map ++ Map('positive_partitions -> value), tail)
      case "--negative_partitions" :: value :: tail =>
        parseMl(map ++ Map('negative_partitions -> value), tail)
      case "--iterations" :: value :: tail =>
        parseMl(map ++ Map('iterations -> value), tail)
      case "--reg_descm" :: value :: tail =>
        parseMl(map ++ Map('reg_descm -> value), tail)
      case "--cluster_k" :: value :: tail =>
        parseMl(map ++ Map('cluster_k -> value), tail)
      case "--input" :: value :: tail =>
        parseMl(map ++ Map('input -> value), tail)
      case "--model_path" :: value :: tail =>
        parseMl(map ++ Map('model_path -> value), tail)
      case "--positiveInput" :: value :: tail =>
        parseMl(map ++ Map('positiveInput -> value), tail)
      case "--negativeInput" :: value :: tail =>
        parseMl(map ++ Map('negativeInput -> value), tail)
      case "--output" :: value :: tail =>
        parseMl(map ++ Map('output -> value), tail)
      case "--work_mode" :: value :: tail =>
        parseMl(map ++ Map('work_mode -> value), tail)
      case "--tokenizer" :: value :: tail =>
        parseMl(map ++ Map('tokenizer -> value), tail)
      case "--file_mode" :: value :: tail =>
        parseMl(map ++ Map('file_mode -> value), tail)
      case "--useLengthFilter" :: value :: tail =>
        parseMl(map ++ Map('useLengthFilter -> value), tail)
      case "--shuffle_partitions" :: value :: tail =>
        parseMl(map ++ Map('shuffle_partitions -> value), tail)
      case "--sql_partitions" :: value :: tail =>
        parseMl(map ++ Map('sql_partitions -> value), tail)
      case _ :: tail =>
        parseMl(map, tail)
    }
  }

  def parseDistinct(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "--input" :: value :: tail =>
        parseDistinct(map ++ Map('input -> value), tail)
      case "--inputL" :: value :: tail =>
        parseDistinct(map ++ Map('inputL -> value), tail)
      case "--inputR" :: value :: tail =>
        parseDistinct(map ++ Map('inputR -> value), tail)
      case "--input_partitions" :: value :: tail =>
        parseDistinct(map ++ Map('input_partitions -> value), tail)
      case "--filter_length" :: value :: tail =>
        parseDistinct(map ++ Map('filter_length -> value), tail)
      case "--sample_count" :: value :: tail =>
        parseDistinct(map ++ Map('sample_count -> value), tail)
      case "--filter_max_length" :: value :: tail =>
        parseDistinct(map ++ Map('filter_max_length -> value), tail)
      case "--stop_words" :: value :: tail =>
        parseDistinct(map ++ Map('stop_words -> value), tail)
      case "--output" :: value :: tail =>
        parseDistinct(map ++ Map('output -> value), tail)
      case "--jaccard_distance" :: value :: tail =>
        parseDistinct(map ++ Map('jaccard_distance -> value), tail)
      case "--sample_rate" :: value :: tail =>
        parseDistinct(map ++ Map('sample_rate -> value), tail)
      case "--haming_distance" :: value :: tail =>
        parseDistinct(map ++ Map('haming_distance -> value), tail)
      case "--sql_partitions" :: value :: tail =>
        parseDistinct(map ++ Map('sql_partitions -> value), tail)
      case "--shuffle_partitions" :: value :: tail =>
        parseDistinct(map ++ Map('shuffle_partitions -> value), tail)
      case "--language_mode" :: value :: tail =>
        parseDistinct(map ++ Map('language_mode -> value), tail)
      case "--language_ratio" :: value :: tail =>
        parseDistinct(map ++ Map('language_ratio -> value), tail)
      case "--stop_size" :: value :: tail =>
        parseDistinct(map ++ Map('stop_size -> value), tail)
      case "--match_mode" :: value :: tail =>
        parseDistinct(map ++ Map('match_mode -> value), tail)
      case "--debug_mode" :: value :: tail =>
        parseDistinct(map ++ Map('debug_mode -> value), tail)
      case "--duplicate_mode" :: value :: tail =>
        parseDistinct(map ++ Map('duplicate_mode -> value), tail)
      case "--line_maxlen" :: value :: tail =>
        parseDistinct(map ++ Map('line_maxlen -> value), tail)
      case "--line_avglen" :: value :: tail =>
        parseDistinct(map ++ Map('line_avglen -> value), tail)
      case "--alpha_frac" :: value :: tail =>
        parseDistinct(map ++ Map('alpha_frac -> value), tail)
      case "--word_flag" :: value :: tail =>
        parseDistinct(map ++ Map('word_flag -> value), tail)
      case "--flag_line" :: value :: tail =>
        parseDistinct(map ++ Map('flag_line -> value), tail)
      case "--tokenizer" :: value :: tail =>
        parseDistinct(map ++ Map('tokenizer -> value), tail)
      case "--tokenizer_mode" :: value :: tail =>
        parseDistinct(map ++ Map('tokenizer_mode -> value), tail)
      case "--file_mode" :: value :: tail =>
        parseDistinct(map ++ Map('file_mode -> value), tail)
      case "--json_field" :: value :: tail =>
        parseDistinct(map ++ Map('json_field -> value), tail)
      case "--separator" :: value :: tail =>
        parseDistinct(map ++ Map('separator -> value), tail)
      case "--tokenize_length" :: value :: tail =>
        parseDistinct(map ++ Map('tokenize_length -> value), tail)
      case _ :: tail =>
        parseDistinct(map, tail)
    }
  }


  def parseES(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "--input" :: value :: tail =>
        parseES(map ++ Map('input -> value), tail)
      case "--input_partitions" :: value :: tail =>
        parseES(map ++ Map('input_partitions -> value), tail)
      case "--filter_length" :: value :: tail =>
        parseES(map ++ Map('filter_length -> value), tail)
      case "--stop_words" :: value :: tail =>
        parseES(map ++ Map('stop_words -> value), tail)
      case "--output" :: value :: tail =>
        parseES(map ++ Map('output -> value), tail)
      case "--jaccard_distance" :: value :: tail =>
        parseES(map ++ Map('jaccard_distance -> value), tail)
      case "--haming_distance" :: value :: tail =>
        parseES(map ++ Map('haming_distance -> value), tail)
      case "--sql_partitions" :: value :: tail =>
        parseES(map ++ Map('sql_partitions -> value), tail)
      case "--shuffle_partitions" :: value :: tail =>
        parseES(map ++ Map('shuffle_partitions -> value), tail)
      case "--language_mode" :: value :: tail =>
        parseES(map ++ Map('language_mode -> value), tail)
      case "--match_mode" :: value :: tail =>
        parseES(map ++ Map('match_mode -> value), tail)
      case "--debug_mode" :: value :: tail =>
        parseES(map ++ Map('debug_mode -> value), tail)
      case "--tokenizer" :: value :: tail =>
        parseES(map ++ Map('tokenizer -> value), tail)
      case "--file_mode" :: value :: tail =>
        parseES(map ++ Map('file_mode -> value), tail)
      case "--separator" :: value :: tail =>
        parseES(map ++ Map('separator -> value), tail)
      case "--nodes" :: value :: tail =>
        parseES(map ++ Map('nodes -> value), tail)
      case "--index" :: value :: tail =>
        parseES(map ++ Map('index -> value), tail)
      case "--tokenize_length" :: value :: tail =>
        parseES(map ++ Map('tokenize_length -> value), tail)
      case _ :: tail =>
        parseES(map, tail)
    }
  }


  def parseStatistics(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "--input" :: value :: tail =>
        parseStatistics(map ++ Map('input -> value), tail)
      case "--input_partitions" :: value :: tail =>
        parseStatistics(map ++ Map('input_partitions -> value), tail)
      case "--filter_length" :: value :: tail =>
        parseStatistics(map ++ Map('filter_length -> value), tail)
      case "--filter_max_length" :: value :: tail =>
        parseStatistics(map ++ Map('filter_max_length -> value), tail)
      case "--stop_words" :: value :: tail =>
        parseStatistics(map ++ Map('stop_words -> value), tail)
      case "--stop_word_path" :: value :: tail =>
        parseStatistics(map ++ Map('stop_word_path -> value), tail)
      case "--filter_words" :: value :: tail =>
        parseStatistics(map ++ Map('filter_words -> value), tail)
      case "--output" :: value :: tail =>
        parseStatistics(map ++ Map('output -> value), tail)
      case "--language_mode" :: value :: tail =>
        parseStatistics(map ++ Map('language_mode -> value), tail)
      case "--match_mode" :: value :: tail =>
        parseStatistics(map ++ Map('match_mode -> value), tail)
      case "--debug_mode" :: value :: tail =>
        parseStatistics(map ++ Map('debug_mode -> value), tail)
      case "--file_mode" :: value :: tail =>
        parseStatistics(map ++ Map('file_mode -> value), tail)
      case "--separator" :: value :: tail =>
        parseStatistics(map ++ Map('separator -> value), tail)
      case "--tokenizer_mode" :: value :: tail =>
        parseStatistics(map ++ Map('tokenizer_mode -> value), tail)
      case _ :: tail =>
        parseStatistics(map, tail)
    }
  }

  def parseDataClean(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "--input" :: value :: tail =>
        parseDataClean(map ++ Map('input -> value), tail)
      case "--input_partitions" :: value :: tail =>
        parseDataClean(map ++ Map('input_partitions -> value), tail)
      case "--filter_length" :: value :: tail =>
        parseDataClean(map ++ Map('filter_length -> value), tail)
      case "--cn_ratio" :: value :: tail =>
        parseDataClean(map ++ Map('cn_ratio -> value), tail)
      case "--language_mode" :: value :: tail =>
        parseDataClean(map ++ Map('language_mode -> value), tail)
      case "--filter_length_max" :: value :: tail =>
        parseDataClean(map ++ Map('filter_length_max -> value), tail)
      case "--filter_length_min" :: value :: tail =>
        parseDataClean(map ++ Map('filter_length_min -> value), tail)
      case "--filter_string" :: value :: tail =>
        parseDataClean(map ++ Map('filter_string -> value), tail)
      case "--convert_string" :: value :: tail =>
        parseDataClean(map ++ Map('convert_string -> value), tail)
      case "--output" :: value :: tail =>
        parseDataClean(map ++ Map('output -> value), tail)
      case "--debug_mode" :: value :: tail =>
        parseDataClean(map ++ Map('debug_mode -> value), tail)
      case "--file_mode" :: value :: tail =>
        parseDataClean(map ++ Map('file_mode -> value), tail)
      case "--tokenizer_mode" :: value :: tail =>
        parseDataClean(map ++ Map('tokenizer_mode -> value), tail)
      case _ :: tail =>
        parseDataClean(map, tail)
    }
  }

  def parseCode(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "--input" :: value :: tail =>
        parseCode(map ++ Map('input -> value), tail)
      case "--input_partitions" :: value :: tail =>
        parseCode(map ++ Map('input_partitions -> value), tail)
      case "--filter_length" :: value :: tail =>
        parseCode(map ++ Map('filter_length -> value), tail)
      case "--stop_words" :: value :: tail =>
        parseCode(map ++ Map('stop_words -> value), tail)
      case "--output" :: value :: tail =>
        parseCode(map ++ Map('output -> value), tail)
      case "--language_mode" :: value :: tail =>
        parseCode(map ++ Map('language_mode -> value), tail)
      case "--match_mode" :: value :: tail =>
        parseCode(map ++ Map('match_mode -> value), tail)
      case "--debug_mode" :: value :: tail =>
        parseCode(map ++ Map('debug_mode -> value), tail)
      case "--separator" :: value :: tail =>
        parseCode(map ++ Map('separator -> value), tail)
      case "--file_mode" :: value :: tail =>
        parseCode(map ++ Map('file_mode -> value), tail)
      case "--tokenizer_mode" :: value :: tail =>
        parseCode(map ++ Map('tokenizer_mode -> value), tail)
      case "--duplicate_mode" :: value :: tail =>
        parseCode(map ++ Map('duplicate_mode -> value), tail)
      case "--line_maxlen" :: value :: tail =>
        parseCode(map ++ Map('line_maxlen -> value), tail)
      case "--line_avglen" :: value :: tail =>
        parseCode(map ++ Map('line_avglen -> value), tail)
      case "--alpha_frac" :: value :: tail =>
        parseCode(map ++ Map('alpha_frac -> value), tail)
      case "--word_flag" :: value :: tail =>
        parseCode(map ++ Map('word_flag -> value), tail)
      case "--flag_line" :: value :: tail =>
        parseCode(map ++ Map('flag_line -> value), tail)
      case _ :: tail =>
        parseCode(map, tail)
    }
  }

}
