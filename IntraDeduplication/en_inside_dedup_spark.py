import argparse
import nltk
import hashlib
import os
import json
import jieba
from pyspark import SparkContext, SparkConf


def hash_text(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def is_repetition_removal(
        text, duplicate_line_fraction=0.3, duplicate_line_character_faction=0.2
):
    line_count = 0
    dup_line = 0
    dup_line_chars = 0
    visit_lines = {}
    for line in text.split("\n"):
        if line.strip() == "":
            continue
        line_hash = hash_text(line)
        if line_hash in visit_lines:
            dup_line += 1
            # print("dup_line", dup_line)
            dup_line_chars += len(line)
        visit_lines[line_hash] = True

        line_count += 1

    if float(dup_line) / line_count > duplicate_line_fraction:
        return True

    if float(dup_line_chars) / len(text) > duplicate_line_character_faction:
        return True

    top_ngram_character_fractions = [
        (2, 0.2),
        (3, 0.18),
        (4, 0.16),
    ]
    for ngram, threshold in top_ngram_character_fractions:
        bgs = nltk.ngrams(text.split(), ngram)
        fdist = nltk.FreqDist(bgs)
        for word_list, repeat in fdist.items():
            char_count = sum([len(word) for word in word_list])
            if char_count * (repeat - 1) / len(text) > threshold:
                return True

    duplicate_ngram_character_fractions = [
        (5, 0.15),
        (6, 0.14),
        (7, 0.13),
        (8, 0.12),
        (9, 0.11),
        (10, 0.10),
    ]
    for ngram, threshold in duplicate_ngram_character_fractions:
        fdist = {}
        word_list = text.split()
        mark = [0] * len(word_list)
        for i in range(len(word_list) - ngram + 1):
            bag = tuple(word_list[i: i + ngram])
            if bag in fdist:
                for j in range(i, i + ngram):
                    mark[j] = len(word_list[j])
                fdist[bag] += 1
            else:
                fdist[bag] = 1

        if sum(mark) / float(len(text)) > threshold:
            return True

    return False


def check_removal(line, duplicate_line_fraction, duplicate_line_character_faction, len_threshold, json_field):
    if len(line) > len_threshold:
        return False
    try:
        json_obj = json.loads(line)
        text = json_obj[json_field]
        is_dedup = is_repetition_removal(text, duplicate_line_fraction, duplicate_line_character_faction)
        return not is_dedup
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}, data: {line}")
        return False


def deleteHdfs(output_path):
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hdfs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    output_dir = sc._jvm.org.apache.hadoop.fs.Path(output_path)
    if hdfs.exists(output_dir):
        print(f"program delete output_dir={output_dir}")
        hdfs.delete(output_dir, True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deduplication Check using PySpark RDD")
    parser.add_argument("--input_path", required=True, help="Path to input JSON file")
    parser.add_argument("--output_path", required=True, help="Path to output clean text file")
    parser.add_argument("--json_field", required=True, help="text or content")
    parser.add_argument("--duplicate_line_fraction", type=float, default=0.3,
                        help="Threshold for duplicate line fraction")
    parser.add_argument("--duplicate_line_character_faction", type=float, default=0.2,
                        help="Threshold for duplicate line character fraction")
    parser.add_argument("--filter_len", type=int, default=100000,
                        help="Threshold for duplicate line character fraction")
    args = parser.parse_args()
    print(
        f"duplicate_line_fraction={args.duplicate_line_fraction}, args.duplicate_line_character_faction={args.duplicate_line_character_faction}")
    input_path = args.input_path
    output_path = args.output_path
    filter_len = args.filter_len
    json_field = args.json_field
    good_output_path = os.path.join(output_path, "dedupClean")
    bad_output_path = os.path.join(output_path, "dedupBad")

    conf = SparkConf().setAppName("EnInsideDedup")
    sc = SparkContext(conf=conf)

    deleteHdfs(output_path)

    input_rdd = sc.textFile(args.input_path, 2000)

    satisfied_rdd = input_rdd.filter(
        lambda x: check_removal(x, args.duplicate_line_fraction, args.duplicate_line_character_faction, filter_len, json_field))

    satisfied_rdd.saveAsTextFile(good_output_path)
    unsatisfied_rdd = input_rdd.sample(False, 0.1).filter(
        lambda x: not check_removal(x, args.duplicate_line_fraction, args.duplicate_line_character_faction, filter_len, json_field))
    unsatisfied_rdd.saveAsTextFile(bad_output_path)
    print(
        f"输入总文档数:{input_rdd.count()}, 留存文档总数:{satisfied_rdd.count()}, 留存率:{satisfied_rdd.count() / input_rdd.count() * 100:.2f}%")
    # 关闭Spark
    sc.stop()