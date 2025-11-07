package component

import algorithm.simhash.{Simhash, SimhashFactory}
import org.utils.Tools

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

class TextSimilarity(hammingDistance: Int = 3, jaccardDistance: Double = 0.1, debugMode: String = "work", useLengthFilter: Boolean = true) extends Serializable {

  /*
      前缀分发，无权重
     */
  def prefixFlatMap(lines: (Long, Array[(Long, Int, Double)])): Iterator[(Long, (Long, Array[Long]))] = {
    val arrayBuffer = new ArrayBuffer[(Long, (Long, Array[Long]))]
    val id = lines._1
    val tokens = lines._2.map(x => x._1)
    val prefix =  tokens.length - scala.math.ceil(tokens.length * (1 - jaccardDistance)).toInt + 1
    if(debugMode.equals("debug")){
      println("prefix = " + prefix + " total len = " + tokens.length + " jaccardDistance = " + jaccardDistance)
    }
    for (i <- 0 until Math.min(prefix, tokens.length)) {
      arrayBuffer.append((tokens.apply(i), (id, tokens)))
    }
    arrayBuffer.iterator
  }

  /*
    内部自验证，无权重
   */
  def groupFlatMap(iterTuple: (Long, Iterable[(Long, Array[Long])])): Iterator[(Long, Long)] = {

    val list = iterTuple._2.toList
    var index_i = 0
    var cmp_count = 0
    val arrayBuffer = new ArrayBuffer[(Long, Long)]()
    while (index_i < list.size){
      val curTuple = list.apply(index_i)
      var index_j = index_i + 1
      while (index_j < list.size) {
        val nextTuple = list.apply(index_j)
        if(!useLengthFilter || nextTuple._2.length >= curTuple._2.length * (1 - jaccardDistance) && nextTuple._2.length <= curTuple._2.length / (1 - jaccardDistance)){
          val tuple_distance = Tools.distanceCal(curTuple._2, nextTuple._2)
          if(debugMode.equals("debug")){
            println("tuple_distance = " + tuple_distance + " jaccardDistance = " + jaccardDistance + " pair = " + curTuple._1 + ":" + nextTuple._1)
            cmp_count = cmp_count + 1
          }

          if(tuple_distance < jaccardDistance){
            arrayBuffer.append((curTuple._1, nextTuple._1))
            //            println("find sim: " + curTuple._1 + "," + nextTuple._1)
            //            println("curTuple2: " + curTuple._2.mkString(","))
            //            println("nextTuple2: " + nextTuple._2.mkString(","))
          }
        }
        index_j = index_j + 1
      }
      index_i = index_i + 1
    }
    if(debugMode.equals("debug")){
      println("cmp_count = " + cmp_count)
    }
    //    println("groupFlatMap: cmp_count=" + cmp_count + " part_no=" + TaskContext.getPartitionId())
    arrayBuffer.iterator
  }


  /*
    前缀分发，idf权重
   */
  def prefixIdfFlatMap(lines: (Long, Array[(Long, Int, Double)])): Iterator[(Long, (Long, Array[(Long, Double)], Double))] = {
    val id = lines._1
    val idf_tokens = lines._2
    var total_weight = 0.0
    for (x <- lines._2){
      total_weight += x._3
    }
    println("lines.toString " + lines.toString())

    val idf_array = idf_tokens.map(x => (x._1, x._3))

    val prefix_weight = total_weight * jaccardDistance
    var sum_weight = 0.0
    val loop = new Breaks
    var index = 0
    loop.breakable {
      while (index < idf_tokens.length) {
        sum_weight += idf_tokens.apply(index)._3
        if (sum_weight > prefix_weight) {
          loop.break()
        }
        index = index + 1
      }
    }
    if(debugMode.equalsIgnoreCase("debug")){
      println("id=" + id + " tokenLen=" + idf_tokens.length + " prefixLen=" + (index+1) + " sum_weight=" + sum_weight + " prefix_weight=" + prefix_weight + " total_weight=" + total_weight)
    }
    //    println("id=" + id + " tokenLen=" + idf_tokens.length + " prefixLen=" + (index+1) + " sum_weight=" + sum_weight + " prefix_weight=" + prefix_weight + " total_weight=" + total_weight)
    //    println("idf_tokens " + idf_tokens.mkString(","))
    val arrayBuffer = new ArrayBuffer[(Long, (Long, Array[(Long, Double)], Double))]
    for(i <- 0 to Math.min(index, idf_tokens.length - 1)){
      arrayBuffer.append((idf_tokens.apply(i)._1, (id, idf_array, total_weight)))
    }
    arrayBuffer.iterator
  }


  /*
    利用flatMap逐条处理group后的数据，每条数据都是一个token对应的多条记录的迭代器，避免整个分区的数据自连接
    内部自连接验证，相对于两表连接，减少一半比较次数和shuffle量。还可以尝试写入其它剪枝
   */
  def groupIdfFlatMap(iterTuple: (Long, Iterable[(Long, Array[(Long, Double)], Double)])): Iterator[(Long, Long)] = {

    val list = iterTuple._2.toList
    var index_i = 0
    var cmp_count = 0
    val arrayBuffer = new ArrayBuffer[(Long, Long)]()
    while (index_i < list.size){
      val curTuple = list.apply(index_i)
      var index_j = index_i + 1
      while (index_j < list.size) {
        val nextTuple = list.apply(index_j)
        val tuple_distance = Tools.distanceCal(curTuple._2, nextTuple._2, curTuple._3, nextTuple._3)
        if(debugMode.equals("debug")){
          println("tuple_distance = " + tuple_distance + " jaccardDistance = " + jaccardDistance + " pair = " + curTuple._1 + ":" + nextTuple._1)
        }
        cmp_count = cmp_count + 1
        if(tuple_distance < jaccardDistance){
          arrayBuffer.append((curTuple._1, nextTuple._1))
          //                      println("find sim: " + curTuple._1 + "," + nextTuple._1)
          //            println("curTuple2: " + curTuple._2.mkString(","))
          //            println("nextTuple2: " + nextTuple._2.mkString(","))
        }
        index_j = index_j + 1
      }
      index_i = index_i + 1
    }
    //    println("groupFlatMap: cmp_count=" + cmp_count + " token=" + iterTuple._1 + " list.size=" + list.length + " part_no=" + TaskContext.getPartitionId())
    arrayBuffer.iterator
  }

  /*
    simhash实现的内部相似度验证
   */
  def groupSimhashFlatMap(iterTuple: (Long, Iterable[(Long, Array[(Long, Int, Double)], Double)])): Iterator[(Long, Long)] = {
    if(debugMode.equalsIgnoreCase("debug")){
      println("groupSimhashFlatMap " + iterTuple._1)
    }
    val iter_list = iterTuple._2.toList
    if(debugMode.equalsIgnoreCase("debug")){
      println(iter_list.map(x => x._1).mkString(","))
    }
    val list = iter_list.map(x => (java.lang.Long.valueOf(x._1), x._2.map(x => java.lang.Long.valueOf(x._1)).toList.asJava)).asJava
    val simList = SimhashFactory.getSimPairs(list, hammingDistance, debugMode).asScala.map(x => (x._1.toLong, x._2.toLong))
    if(debugMode.equalsIgnoreCase("debug") && simList.length > 0) {
      println("simList: " + simList.mkString(","))
    }
    simList.iterator
  }

  /*
    simhash带权重
   */
  def groupSimhashFlatMapWithWeightFromList(iterTuple: (Long, Iterable[(Long, Array[(Long, Int, Double)], Double)]), matchMode: String = "fuzzy"): Iterator[(Long, Long)] = {

    if(debugMode.equalsIgnoreCase("debug")){
      println("groupSimhashFlatMap " + iterTuple._1)
    }
    val iter_list = iterTuple._2.toList
    if(debugMode.equalsIgnoreCase("debug")){
      println(iter_list.map(x => x._1).mkString(","))
    }

    val list = iter_list.map(x => (java.lang.Long.valueOf(x._1), x._2.map(x => (java.lang.Long.valueOf(x._1), java.lang.Integer.valueOf(x._2), java.lang.Double.valueOf(x._3))).toList.asJava)).asJava
    val simList = SimhashFactory.getSimWeightPairsFromList(list, hammingDistance, matchMode, debugMode).asScala.map(x => (x._1.toLong, x._2.toLong))
    if(debugMode.equalsIgnoreCase("debug") && simList.length > 0) {
      println("simList: " + simList.mkString(","))
    }
    simList.iterator
  }

  /*
    simhash+海明距离进行相似度验证，返回相似对
   */
  def groupSimhashFlatMapWithWeight(iterTuple: (Long, Iterable[(Long, Long)]), matchMode: String = "fuzzy"): Iterator[(Long, Long)] = {

    if(debugMode.equalsIgnoreCase("debug")){
      println("groupSimhashFlatMap " + iterTuple._1)
    }
    val iter_list = iterTuple._2.toList
    //      .sortWith((x, y) => x._1 < y._1)
    if(debugMode.equalsIgnoreCase("debug")){
      println(iter_list.map(x => x._1).mkString(","))
    }

    val list = iter_list.map(x => (java.lang.Long.valueOf(x._1), java.lang.Long.valueOf(x._2))).asJava
    val simList = SimhashFactory.getSimWeightPairs(list, hammingDistance, matchMode, debugMode).asScala.map(x => (x._1.toLong, x._2.toLong))
    if(debugMode.equalsIgnoreCase("debug") && simList.length > 0) {
      println("simList: " + simList.mkString(","))
    }
    simList.iterator
  }



  /*
    全文本分布式simhash
 */
  def flatMapWithSimHash(lines: (Long, Array[(Long, Int, Double)]), simHash: Simhash): Iterator[(Long, (Long, Long))] = {
    val id = lines._1
    val idf_tokens = lines._2
    var total_weight = 0.0
    for (x <- lines._2){
      total_weight += x._3
    }

    val list = idf_tokens.map(x => (java.lang.Long.valueOf(x._1), java.lang.Integer.valueOf(x._2), java.lang.Double.valueOf(x._3))).toList.asJava
    val simCode = simHash.simhash64WithWeight(list)
    val arrayBuffer = new ArrayBuffer[(Long, (Long, Long))]
    for(i <- 0 until idf_tokens.length){
      arrayBuffer.append((idf_tokens.apply(i)._1, (id, simCode)))
    }
    arrayBuffer.iterator
  }

  /*
    根据jaccard相似度计算绝对前缀并分发数据
   */
  def prefixFlatMapWithSimHash(lines: (Long, Array[(Long, Int, Double)]), simHash: Simhash): Iterator[(Long, (Long, Long))] = {
    val id = lines._1
    val idf_tokens = lines._2
    var total_weight = 0.0
    for (x <- lines._2){
      total_weight += x._3
    }
    val list = idf_tokens.map(x => (java.lang.Long.valueOf(x._1), java.lang.Integer.valueOf(x._2), java.lang.Double.valueOf(x._3))).toList.asJava
    val simCode = simHash.simhash64WithWeight(list)

    val prefix_weight = total_weight * jaccardDistance
    var sum_weight = 0.0
    val loop = new Breaks
    var index = 0
    loop.breakable {
      while (index < idf_tokens.length) {
        sum_weight += idf_tokens.apply(index)._3
        if (sum_weight > prefix_weight) {
          loop.break()
        }
        index = index + 1
      }
    }
    if(debugMode.equalsIgnoreCase("debug")){
      println("id=" + id + " tokenLen=" + idf_tokens.length + " prefixLen=" + (index+1) + " sum_weight=" + sum_weight + " prefix_weight=" + prefix_weight + " total_weight=" + total_weight)
    }
    //    println("id=" + id + " tokenLen=" + idf_tokens.length + " prefixLen=" + (index+1) + " sum_weight=" + sum_weight + " prefix_weight=" + prefix_weight + " total_weight=" + total_weight)
    //    println("idf_tokens " + idf_tokens.mkString(","))
    val arrayBuffer = new ArrayBuffer[(Long, (Long, Long))]
    for(i <- 0 to Math.min(index, idf_tokens.length - 1)){
      arrayBuffer.append((idf_tokens.apply(i)._1, (id, simCode)))
    }
    arrayBuffer.iterator
  }



}
