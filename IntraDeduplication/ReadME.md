# IntraDeduplication Methods and Run Instructions

## I. Introduction

In-sample deduplication focuses on cleaning repeated text fragments that
appear within a single sample.\
This method is inspired by the cleaning ideas from the [C4 dataset
script](https://github.com/shjwudp/c4-dataset-script), with improvements
based on issues found during data evaluation and characteristics
specific to our dataset.

------------------------------------------------------------------------

## II. Run Instructions

### 1. Runtime Environment

    Spark  : 3.0.1
    Hadoop : 2.10.1
    Python : 3.8.3
    nltk   : 3.8.1
    jieba  : 0.42.1

------------------------------------------------------------------------

### 2. Run Command

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

#### Notes

-   Configure *executors*, *driver_memory*, and other Spark parameters
    according to your cluster resources.
-   Use **cn_inside_dedup_spark.py** for Chinese text.
-   Use **en_inside_dedup_spark.py** for English text.

### Parameter Description

  |Parameter                              |Description                                      |Default|
  |:-------------------------------------- |:------------------------------------------------ |:---------|
  |**json_field**                         |Name of the text field                           |text|
  |**duplicate_line_fraction**            |Threshold for proportion of duplicated lines; deduplication is applied when exceeded   |0.3|
  |**duplicate_line_character_faction**   |Threshold for proportion of duplicate-line characters relative to total characters     |0.2|
