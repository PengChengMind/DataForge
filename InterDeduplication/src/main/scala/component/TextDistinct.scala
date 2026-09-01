package component

import algorithm.setmerge.SetMergeLong
import com.alibaba.fastjson.JSONObject
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{conv, explode}
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import org.utils.Tools

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class TextDistinct(rootPath: String, debugMode: String = "work") extends Serializable {

  val logger = LoggerFactory.getLogger(this.getClass)

  def seqOp(acc: ArrayBuffer[Set[Long]], input: Set[Long]): ArrayBuffer[Set[Long]] = {
    var flag = false
    for ((ele, i) <- acc.zipWithIndex) {
      //      println("ele, i : " + ele + "," + i + " input: " + input)
      if ((ele & input).size > 0){
        flag = true
        acc(i) = acc(i).|(input)
        //        println("acc&ele&input: " + acc + " " + ele + " " + input)
      }
    }

    if(!flag){
      acc.+:(input)
    } else {
      acc
    }
  }

  /*
  暴力合并，维护一个相似集合的数组保存累计结果，遍历每一个相似对，每个相似对都需要和累计结果的每个相似集合进行匹配，匹配则合并
 */
  def combinePartitionSet(it: Iterator[Set[Long]]): Iterator[Set[Long]] = {
    var acc = ArrayBuffer[Set[Long]]()
    for (obj <- it){
      //      println("acc-obj: " + acc + " " + obj)
      acc = seqOp(acc, obj)
    }
    //    println("acc: " + acc)
    acc.iterator
  }

  /*
    算法合并
 */
  def combinePartitionPair(it: Iterator[Array[Long]]): Iterator[Array[Long]] = {

    val arrays = it.map(x => x.map(x => java.lang.Long.valueOf(x))).toArray
    val disjointSet = new SetMergeLong(debugMode.equalsIgnoreCase("debug"))
    val mergedSetList = disjointSet.disjoin(arrays)
      .asScala
      .filter(_.size() > 0)
      .map(x => x.toArray.map(x => x.toString.toLong))
    mergedSetList.iterator
  }

  def saveDeduplicateLineResult(sim_rdd: RDD[(Long, Long)], lines_ids: RDD[(String, Long)], spark: SparkSession, sc: SparkContext, fileMode: String = "line"): (Int, Long, RDD[String]) = {
    /*
      收集所有分区的相似对形成若干不相交的set集合
     */
    val sim_set_df = sim_rdd.map(x => Array(x._1, x._2))
    val collection = sim_set_df.mapPartitions(combinePartitionPair).collect()

    val partitionIterator = combinePartitionPair(collection.iterator)
    var duplicateIds = Set[Long]()
    var duplicateSeq = Seq[(Long, List[Long])]()
    for (array_ele <- partitionIterator){

      val ele = array_ele.toSet
      val reserve = ele.min
      val duplicate = ele.-(reserve)
      duplicateSeq = duplicateSeq.+:(reserve, array_ele.toList)
      duplicateIds = duplicateIds.|(duplicate)
//      logger.info("duplicateIds size " + duplicateIds.size + " added size:" + duplicate.size + " added: " + duplicate.mkString(","))
    }

    val id_lines = lines_ids.map(x => (x._2, x._1))

    val duplicate_output_2 = sc.parallelize(duplicateSeq)
    val duplicate_detail_rdd_2 = duplicate_output_2.filter(x => x._2.length > 1)
      .flatMap(x => x._2.map(id => (id, x._1)))
      .join(id_lines)
      .map(x => (x._2._1, x._1 + "\n" + x._2._2))
      .groupByKey()
      .map(x => x._2.mkString("\n") + "\n")

//    println("duplicate_output_2.show(3)")
//    duplicate_output_2.take(3).foreach(println)
    duplicate_output_2.map(x => x._2.mkString(","))
      .saveAsTextFile(rootPath + "/duplicate_output_2")

    val broad_duplicateIds = sc.broadcast(duplicateIds)
    val duplicate_fuzzy_size = duplicateIds.size
    duplicate_detail_rdd_2.saveAsTextFile(rootPath + "/duplicate_detail_output_2")
    //筛选重复的文档
    var reservedDocs = lines_ids.filter(x => !broad_duplicateIds.value.contains(x._2))
      .map(x => x._1)
    reservedDocs = if (fileMode.equalsIgnoreCase("json")){
      reservedDocs.map( x => {
        val json = new JSONObject()
        json.put("text", x)
        json.toJSONString
      })
    } else {
      reservedDocs.map(x => x + "\n")
    }
    if(debugMode.equalsIgnoreCase("debug")) {
      println("reservedDocs.take(10).foreach(println)")
      reservedDocs.take(10).foreach(println)
    }
    Tools.saveRdd(sc, reservedDocs, rootPath + "/reserved_output")
    val reserved_size = reservedDocs.count()
    (duplicate_fuzzy_size, reserved_size, duplicate_detail_rdd_2)
  }

  def saveDeduplicateFileResult(sim_rdd: RDD[(Long, Long)], lines_ids: RDD[(String, Long)], file_ids: RDD[(Long, String)], spark: SparkSession, sc: SparkContext): Dataset[Row] = {
    /*
      收集所有分区的相似对形成若干不相交的set集合
     */
    val sim_set_df = sim_rdd.map(x => Array(x._1, x._2))
    val collection = sim_set_df.mapPartitions(combinePartitionPair).collect()

    val partitionIterator = combinePartitionPair(collection.iterator)
    var duplicateIds = Set[Long]()
    var duplicateSeq = Seq[(Long,String)]()
    for (array_ele <- partitionIterator){

      val ele = array_ele.toSet
      val reserve = ele.min
      val duplicate = ele.-(reserve)
      duplicateSeq = duplicateSeq.+:(reserve, duplicate.mkString(","))
      duplicateIds = duplicateIds.|(duplicate)
      println("duplicateIds size " + duplicateIds.size + " added size:" + duplicate.size + " added: " + duplicate.mkString(","))
    }
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val duplicateDf = spark.createDataFrame(duplicateSeq)
      .toDF("reserved", "removed")
      .withColumn("removed", explode(split($"removed", ",")))

    val file_ids_schema: StructType = StructType(Array(
      StructField("id", LongType),
      StructField("desc", StringType)
      ),
    )
    val file_ids_row = file_ids.map(x => Row(x._1, x._2))
    val file_ids_df = spark.createDataFrame(file_ids_row, file_ids_schema)
    duplicateDf.show()
    file_ids_df.show()


    val duplicate_files_df = duplicateDf.alias("df1").join(file_ids_df.alias("df2"), $"df1.removed" === $"df2.id")
      .join(file_ids_df.alias("df3"), $"df1.reserved" === $"df3.id")
      .select($"df2.desc".alias("removed"), $"df3.desc".alias("reserved"))

    if(debugMode.equalsIgnoreCase("debug")) {
      println("duplicate_files_df.show(10)")
      duplicate_files_df.show(10)
    }
    Tools.deleteHdfsPath(sc, rootPath + "/result_output")
    duplicate_files_df.write.csv(rootPath + "/result_output")
    duplicateDf
  }
}
