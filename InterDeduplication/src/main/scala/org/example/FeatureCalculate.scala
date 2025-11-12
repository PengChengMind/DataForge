package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.{SparkConf, SparkContext}

object FeatureCalculate {

  //定义主方法
  def main(args: Array[String]): Unit = {


    //创建SparkConf对象
    //如果Master是local，表示运行在本地模式上，即可以在开发工具中直接运行
    //如果要提交到集群中运行，不需要设置Master
    //集群模式
    //val conf = new SparkConf().setAppName("My Scala Word Count")

    //    var input = "D:\\vmshare\\ubuntu1\\air_train_data.csv"
    var input = "input/air_train_data_1000.csv"
    var output = "output/air_result"
    //本地模式
    var conf = new SparkConf().setAppName("My Scala Word Count").setMaster("local")
    if(args.length == 2){
      input = args(0)
      output = args(1)
      conf = new SparkConf().setAppName("My Scala Word Count")
    }
    print("version 1.0 input = " + input + " output = " + output)

    //创建SparkContext对象
    val sc = new SparkContext(conf)
    //    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName).master("local")
      .getOrCreate()

    var dfCsv = spark.read.format("csv")
      .option("header", true)
      .option("inferschema", "true")
      //      .option("maxColumns", 1000)
      .load(input)

    import org.apache.spark.sql.functions._
    dfCsv.printSchema()
    //数据原始情况
    println("columns count = " + dfCsv.columns.length)
    println("lines count = " + dfCsv.count())

    val re = dfCsv.show(100)

    //数据采样
    dfCsv.sample(0.01).show()

    // 各维度特征数量统计
    val re2 = dfCsv.select(dfCsv.columns.map(c => countDistinct(col(c)).alias(c)): _*).show(100)

    // 指定维度的特征分布统计
    val re3 = dfCsv.groupBy("pm25").count().orderBy(col("count").desc).show(100)

    // 空值替换
    val re_null = dfCsv.na.fill("0").na.replace(dfCsv.columns, Map("NA" -> "0", "NW" ->"Test", "null" -> "0")).show(100)

    // 类型转换
    dfCsv = dfCsv.withColumn("DEWP_temp", dfCsv.col("DEWP").cast(IntegerType))
      .drop("DEWP")
      .withColumnRenamed("DEWP_temp", "DEWP")

    // 归一化
    //      dfCsv.groupBy().agg(max("DEWP"), min("DEWP")).show(100)
    val min_max = dfCsv.groupBy().agg(max("DEWP"), min("DEWP")).collect()(0)
    println(min_max.schema)
    val DEWP_MAX = min_max.getInt(0)
    val DEWP_MIN = min_max.getInt(1)

    dfCsv = dfCsv.withColumn("DEWP_temp", (dfCsv.col("DEWP") - DEWP_MIN)/(DEWP_MAX - DEWP_MIN))
      .drop("DEWP")
      .withColumnRenamed("DEWP_temp", "DEWP")
    dfCsv.show(200)
    dfCsv.write.format("csv").mode("append").save(output)
    sc.stop()

  }

}
