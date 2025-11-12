package org.ml

import java.io.File
import java.util

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object MailClassification {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[5]").setAppName("EnglishSpam")
    val context = new SparkContext(conf)
    val sess = SparkSession.builder().config(conf).getOrCreate()
    // 读取数据集
    val lines = context.textFile("C:\\Users\\asus\\Desktop\\data\\spam_ham.txt")
    // 处理准备数据
    val rowRDD = lines.map{
      line =>
        val arr = line.split("\t")
        val types = if(arr(0) == "ham") 0.0 else 1.0
        Row(types, arr(1))
    }
    val fields = Array[StructField](
      new StructField("label", DataTypes.DoubleType,true),
      new StructField("content", DataTypes.StringType,true)
    )
    val schema = new StructType(fields)
    val data = sess.createDataFrame(rowRDD,schema)
    // 分割数据
    // 分词器
    val words = new Tokenizer().setInputCol("content").setOutputCol("words")
    // 将分词装入hash桶中，统计词频，转化为特征向量数据
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol("words").setOutputCol("features")
    // 创建逻辑算法对象，用以训练hash桶向量数据
    val logisticRegression = new LogisticRegression().setMaxIter(20).setRegParam(0.1)
    // 管道对象，将三个步骤拟合连接
    val pip = new Pipeline().setStages(Array(words, hashingTF, logisticRegression))
    // 拟和数据，产生模型
    val model = pip.fit(data)
    // 持久化模型
    val modelPath = "C:\\Users\\asus\\Desktop\\data\\email\\english\\model"
    val file = new File(modelPath)
    if(file.list().length == 0) model.save(modelPath + "\\scala")
    // 测试
    val testList = new util.ArrayList[String]()
    testList.add("Get me out of this dump heap. My mom decided to come to lowes. BORING.")
    testList.add("REMINDER FROM O2: To get 2.50 pounds free call credit and details of great offers pls reply 2 this text with your valid name, house no and postcode")
    testList.add("Cool, what time you think you can get here?")
    import sess.implicits._
    val testSet =sess.createDataset(testList).withColumnRenamed("value", "content")
    val predict = model.transform(testSet)
    predict.createTempView("res")
    sess.sql("select content,prediction from res").show()
  }
}

