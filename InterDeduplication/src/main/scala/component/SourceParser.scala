package component

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import tokenizer.ITokenizer

class SourceParser extends Serializable{

  /*
    行模式：从输入文件中按行获取数据，进行基本分区和过滤，输出[(行数据，编号)]
   */
  def getIndexedRddForLine(inputPath: String, repartitionNum: Int = 200, filterLength: Int = 0, filterMaxLength: Int = 0, spark: SparkSession, sc: SparkContext): RDD[(String, Long)] = {
    var lines = sc.textFile(inputPath)
    if (repartitionNum > 0) {
      lines = lines.repartition(repartitionNum)
    }
    lines.zipWithIndex()
      .filter(x => x._1.length > filterLength && x._1.length <= filterMaxLength)
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  /*
  段落模式：从输入文件中按分割符(\n\n)获取数据，进行基本分区和过滤，输出[(行数据，编号)]
 */
  def getIndexedRddForPara(inputPath: String, repartitionNum: Int = 200, filterLength: Int = 0, filterMaxLength: Int = 0, separator: String = "\n\n", spark: SparkSession, sc: SparkContext): RDD[(String, Long)] = {
    // 重分区
    var files = sc.wholeTextFiles(inputPath, 1000)
    if (repartitionNum > 0) {
      files = files.repartition(repartitionNum)
    }
    files.flatMap(x => x._2.split(separator))
      .zipWithIndex()
      .filter(x => x._1.length > filterLength && x._1.length <= filterMaxLength)
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  /*
    文件模式：从输入文件中按文件获取数据，进行基本分区和过滤，输出[(文件名，文件内容，编号)]
 */
  def getIndexedRddForFile(inputPath: String, repartitionNum: Int = 200, filterLength: Int = 0, filterMaxLength: Int = Int.MaxValue, spark: SparkSession, sc: SparkContext): RDD[((String,String), Long)] = {
    // 重分区
    sc.wholeTextFiles(inputPath)
      .filter(x => x._2.length > filterLength && x._2.length <= filterMaxLength)
      .zipWithIndex()
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  /*
    行模式：获取带有feature的DataFrame，包括[id,text,desc,feature]
    desc表示
   */
  def getFeaturedDfForLine(inputPath: String, repartitionNum: Int = 200, filterLength: Int = 0, filterMaxLength: Int = Int.MaxValue, hashingTF: HashingTF, tokenizer: ITokenizer, spark: SparkSession, sc: SparkContext): DataFrame = {
    // 获取编号
    val lines_ids = getIndexedRddForLine(inputPath, repartitionNum, filterLength, filterMaxLength, spark, sc)
    // 分词
    val ids_tokenss_split = lines_ids
      .map(x => (x._2, tokenizer.tokens(x._1).toArray, x._1))
    val ids_tokenss = ids_tokenss_split
      .map(x => Row(x._1, x._2, x._3))
    // DF模式
    val ids_tokenss_schema: StructType = StructType(Array(
      StructField("id", LongType),
      StructField("text", ArrayType(StringType)),
      StructField("desc", StringType)
    ),
    )
    // RDD 转 DataFrame
    val df = spark.createDataFrame(ids_tokenss, ids_tokenss_schema)
    //生成特征
    hashingTF.transform(df)
  }

  //文件模式：获取带有feature的DataFrame，包括[id,text,desc,feature]
  //desc表示文件名
  def getFeaturedDfForFile(inputPath: String, repartitionNum: Int = 200, filterLength: Int = 0, filterMaxLength: Int = Int.MaxValue, hashingTF: HashingTF, tokenizer: ITokenizer, spark: SparkSession, sc: SparkContext): DataFrame = {
    //获取编号RDD
    val file_ids = getIndexedRddForFile(inputPath, repartitionNum, filterLength, filterMaxLength, spark, sc)
//    file_ids.take(3).foreach(println)
    //分词
    val ids_tokenss_split = file_ids
      .map(x => (x._2, tokenizer.tokens(x._1._2).toArray, x._1._1))
    // 转换为Row类型
    val ids_tokenss = ids_tokenss_split
      .map(x => Row(x._1, x._2, x._3))
    val ids_tokenss_schema: StructType = StructType(Array(
      StructField("id", LongType),
      StructField("text", ArrayType(StringType)),
      StructField("desc", StringType)
    ),
    )
    // RDD 转 DataFrame
    val df = spark.createDataFrame(ids_tokenss, ids_tokenss_schema)
    //生成特征
    hashingTF.transform(df)
  }
}
