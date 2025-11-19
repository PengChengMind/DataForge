# InterDeduplication Methods and Run Instructions

## I. Introduction

Deduplication between samples includes two categories, both implemented
based on the Spark distributed processing framework:

### 1. Fuzzy Deduplication

Uses distributed Simhash, prefix pruning, and related techniques. It
determines similarity based on text Jaccard distance and Hamming
distance, allowing removal of similar texts. This method is suitable for
scenarios sensitive to repeated content.

### 2. Fast Deduplication

Based on characteristics of the data and the deduplication requirements
of LLMs, this method applies rule-based rapid deduplication. It is
suitable for scenarios with high performance requirements.

------------------------------------------------------------------------

## II. Run Instructions

### 1. Runtime Environment

    Spark  : 3.0.1  
    Hadoop : 2.10.1  
    Scala  : 2.12.12

The code is mainly written in Scala (natively supported by Spark). Some
functions are implemented in Java. Required dependencies are organized
through Maven and can be assembled into an executable JAR using the
assembly plugin.

------------------------------------------------------------------------

### 2. Fuzzy Deduplication

### Run Command

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

#### Parameter Description

Please configure parameters such as *executors*, *driver_memory*, etc.,
according to your cluster resources.

  |Parameter              |Description                                  |Default|
  |:---------------------- |:-------------------------------------------- |:---------|
  |**haming_distance**    |Hamming distance threshold; larger = looser matching  |3|
  |**jaccard_distance**   |Jaccard distance threshold; larger = looser matching  |0.1|
  |**file_mode**          |Recommended to use `json`                    |json|
  |**json_field**         |Text field name                              |text|
  |**language_mode**      |cn: Chinese; en: English                     |cn|
  |**tokenizer_mode**     |Chinese: jieba; English: sep (whitespace)    |sep|

------------------------------------------------------------------------

### 3. Fast Deduplication

### Run Command

``` bash
spark-submit   \
    --class tool.distinct.LanguageExactDuplicate   \
    --master yarn   \
    --deploy-mode client   \
    --num-executors ${executors}   \
    --driver-memory ${driver_memory}   \
    --executor-memory ${executor_memory}   \
    --executor-cores ${executor_cores}   \
    SparkScalaProjects-3.1-SNAPSHOT-jar-with-dependencies.jar   \
    --input $outside_input_path   \
    --output $outside_output_path   \
    --file_mode json   \
    --json_field text   \
    --language_mode cn   \
    --tokenizer_mode jieba   \
    --stop_size 200
```

#### Parameter Description


  |Parameter           | Description                                                   |Default|
  |:--------------------|:--------------------------------------------------------------|:---------|
  |**file_mode**        | Recommended to use `json`                                     |json|
  |**json_field**       | Text field name                                               |text|
  |**language_mode**    | cn: Chinese; en: English                                      |cn|
  |**tokenizer_mode**   | Chinese: jieba; English: sep                                  |jieba|
  |**stop_size**        | Threshold for filtering high-frequency tokens;larger = looser |200|
                                                         

