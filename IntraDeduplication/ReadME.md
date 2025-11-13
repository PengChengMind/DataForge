# 样本内去重方法与运行说明

## 一、样本内去重方法

样本内去重是针对样本内部重复较多的文字片段的清洗流程，借鉴了[C4 Data Script](https://github.com/shjwudp/c4-dataset-script)提供的样本内重复内容清洗思路，结合我们在评估数据过程中遇到的问题加以改进，并基于Spark分布式处理框架实现进行提速：


## 二、运行方式

### 1. 运行环境

    Spark  : 3.0.1  
    Hadoop : 2.10.1  
    Python  : 3.8.3
    nltk : 3.8.1
    jieba : 0.42.1
------------------------------------------------------------------------
nltk: https://www.nltk.org/data.html
### 2. 运行命令

``` bash
spark-submit \
     --master yarn \
     --deploy-mode cluster \
     --archives hdfs:///user/pcl/input/anaconda_envs/pyspark_pangu.zip#pyspark_pangu \
     --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=pyspark_pangu/pyspark_pangu/bin/python \
     --num-executors $executors \
     --driver-memory ${driver_memory} \
     --executor-memory ${executor_memory} \
     --executor-cores ${executor_cores} \
     cn_inside_dedup_spark.py \
     --input_path $inside_input_path \
     --output_path $inside_output_path \
     --json_field text \
     --duplicate_line_fraction 0.3 \
     --duplicate_line_character_faction 0.2
```

#### 主要参数说明
其中executors、driver_memory等参数请根据自己的集群资源情况进行配置
中文使用：cn_inside_dedup_spark.py
英文使用：en_inside_dedup_spark.py
 | 参数                  | 说明                                   | 默认值|
 |:-----------------------|:---------------------------------------|:--------|
 | **json_field**         |文本字段名                              |text|
 | **duplicate_line_fraction**      |重复行比例阈值，当重复的行占总行数超过一定比例时，则进行去重处理                      |0.3|
 | **duplicate_line_character_faction**     |重复行字符比例阈值，当重复行的字符数占样本总字符数超过一定比例时，则进行去重处理        |0.2|

------------------------------------------------------------------------

