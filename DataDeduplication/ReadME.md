# 数据去重方法与运行说明

## 一、数据去重方法

数据去重包含两类方法，均基于Spark分布式处理框架实现：

### 1. 模糊去重（Simhash + 前缀剪枝）

采用分布式Simhash、前缀剪枝等技术，对相似文本进行模糊判定和去重。

### 2. 快速去重（基于规则）

根据数据特征和LLM对重复度的要求，通过特定规则实现高效去重，适用于对速度要求较高的场景。

------------------------------------------------------------------------

## 二、运行方式

### 1. 运行环境

    Spark  : 3.0.1  
    Hadoop : 2.10.1  
    Scala  : 2.12.12
------------------------------------------------------------------------
代码主要基于Spark原生支持的Scala语言构建，部分函数方法通过java实现，所需依赖通过maven仓库组织，可通过assembly打包，生成可执行jar包。

### 2. 模糊去重

### 运行命令

``` bash
spark-submit  \
   --class tool.distinct.SimhashPrefixIdfDuplicate   \
   --master yarn   \
   --deploy-mode client  \ 
   --num-executors ${executors}  \ 
   --driver-memory ${driver_memory} \  
   --executor-memory ${executor_memory} \  
   --executor-cores ${executor_cores}   \
   SparkScalaProjects-3.0-SNAPSHOT-jar-with-dependencies.jar \  
   --input $input_path   \
   --output $output_path  \ 
   --haming_distance $ham  \ 
   --jaccard_distance $distance \  
   --file_mode json   \
   --json_field text   \
   --language_mode cn   \
   --tokenizer_mode jieba
```

#### 主要参数说明
其中executors、driver_memory等参数请根据自己的集群资源情况进行配置
 | 参数                  | 说明                                   | 默认值|
 |:-----------------------|:---------------------------------------|:--------|
 | **haming_distance**    |样本间海明距离，越大模糊判定阈值越松    |3|
 | **jaccard_distance**   |样本间 Jaccard 距离，越大重复判定越松   |0.1|
 | **file_mode**          |建议统一使用 json                       |json|
 | **json_field**         |文本字段名                              |text|
 | **language_mode**      |cn：中文；en：英文                      |cn|
 | **tokenizer_mode**     |中文：jieba；英文：sep（空白符）        |sep|

------------------------------------------------------------------------

### 3. 快速去重

### 运行命令

``` bash
spark-submit   
    --class tool.distinct.LanguageExactDuplicate   \
    --master yarn   \
    --deploy-mode client   \
    --num-executors ${executors}   \
    --driver-memory ${driver_memory}   \
    --executor-memory ${executor_memory}   \
    --executor-cores ${executor_cores}   \
    spark-script/SparkScalaProjects-3.1-SNAPSHOT-jar-with-dependencies.jar   \
    --input $outside_input_path   \
    --output $outside_output_path   \
    --file_mode json   \
    --json_field text   \
    --language_mode cn   \
    --tokenizer_mode jieba   \
    --stop_size 200
```

#### 主要参数说明

 | 参数                 |说明                                  | 默认值|
 |:--------------------|:------------------------------------|:------|
 | **file_mode**       |建议统一使用 json                       |json|
 | **json_field**      |文本字段名                              |text|
 | **language_mode**   |cn：中文；en：英文                       |cn|
 | **tokenizer_mode**  |中文：jieba；英文：sep                   |jieba|
 | **stop_size**       |模糊程度阈值，越大过滤高频词越多，重复判定越松  |200|
