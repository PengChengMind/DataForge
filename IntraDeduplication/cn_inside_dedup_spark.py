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
    max_repeat_line_num = 0
    max_repeart_line = ""
    previous_line = ""
    previous_continous = 1
    for line in text.split("\n"):
        line = line.strip()
        if line == "":
            continue
        line_hash = hash_text(line.strip())
        if line != "" and previous_line == line_hash:
            previous_continous += 1
        else:
            previous_continous = 1
        if previous_continous >= 2:
            return True
        if line_hash in visit_lines:
            dup_line += 1
            dup_line_chars += len(line)
            visit_lines[line_hash] += 1
            if visit_lines[line_hash] > max_repeat_line_num:
                max_repeat_line_num = visit_lines[line_hash]
                max_repeart_line = line
        else:
            visit_lines[line_hash] = 1
        previous_line = line_hash

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
        word_list = list(jieba.cut(text))
        bgs = nltk.ngrams(word_list, ngram)
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
        word_list = list(jieba.cut(text))
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


def check_removal(line, duplicate_line_fraction, duplicate_line_character_faction, json_field):
    try:
        json_obj = json.loads(line)
        text = json_obj[json_field]
        if len(text.strip()) < 10:
            return False
        is_dedup = is_repetition_removal(text, duplicate_line_fraction, duplicate_line_character_faction)
        return not is_dedup
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}, data: {line}")
        return False
def remove_repetition_removal(
    text, duplicate_line_fraction, duplicate_line_character_faction, json_field
):
    line_count = 0
    dup_line = 0
    dup_line_chars = 0
    visit_lines = {}
    max_repeat_line_num = 0
    max_repeart_line = ""
    text_new = ""
    for line in text.split("\n"):
        line = line.strip()
        if line == "":
            continue
        line_hash = hash_text(line.strip())
        if line_hash in visit_lines:
            visit_lines[line_hash] += 1
            print("line", line, "dup_line", dup_line)
        else:
            text_new += line + "\n"
            visit_lines[line_hash] = 1

        line_count += 1


    top_ngram_character_fractions = [
        (2, 0.2),
        (3, 0.18),
        (4, 0.16),
    ]
    for ngram, threshold in top_ngram_character_fractions:
        word_list = list(jieba.cut(text_new))
        bgs = nltk.ngrams(word_list, ngram)
        fdist = nltk.FreqDist(bgs)
        for word_list, repeat in fdist.items():

            char_count = sum([len(word) for word in word_list])
            if char_count * (repeat - 1) / len(text_new) > threshold:
                return True, text_new

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
        word_list = list(jieba.cut(text_new))
        mark = [0] * len(word_list)
        # print("word_list", word_list)
        # print("len(word_list)", len(word_list))
        for i in range(len(word_list) - ngram + 1):
            bag = tuple(word_list[i: i + ngram])
            if bag in fdist:
                for j in range(i, i + ngram):
                    mark[j] = len(word_list[j])
                fdist[bag] += 1
            else:
                fdist[bag] = 1

        if sum(mark) / float(len(text_new)) > threshold:
            return True, text_new

    return False, text_new



def remove_removal(line, duplicate_line_fraction, duplicate_line_character_faction, json_field):
    try:
        json_obj = json.loads(line)
        text = json_obj[json_field]
        if len(text.strip()) < 10:
            return False, line
        is_dedup, text_new = remove_repetition_removal(text, duplicate_line_fraction, duplicate_line_character_faction, json_field)
        json_obj[json_field] = text_new
        return not is_dedup, json.dumps(json_obj, ensure_ascii=False)
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}, data: {line}")
        return False, line


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deduplication Check using PySpark RDD")
    parser.add_argument("--input_path", required=True, help="Path to input JSON file")
    parser.add_argument("--output_path", required=True, help="Path to output clean text file")
    parser.add_argument("--json_field", required=True, help="text or content")
    parser.add_argument("--remove_line", required=True, type=int, default=0, help="wheather remove dup line")
    parser.add_argument("--duplicate_line_fraction", type=float, default=0.3,
                        help="Threshold for duplicate line fraction")
    parser.add_argument("--duplicate_line_character_faction", type=float, default=0.2,
                        help="Threshold for duplicate line character fraction")
    args = parser.parse_args()

    input_path = args.input_path
    output_path = args.output_path
    json_field = args.json_field
    remove_line = args.remove_line
    good_output_path = os.path.join(output_path, "dedupClean")
    bad_output_path = os.path.join(output_path, "dedupBad")

    conf = SparkConf().setAppName("DeduplicationCheck")
    sc = SparkContext(conf=conf)

    input_rdd = sc.textFile(args.input_path, 2000)


    if remove_line == 1:
        print("use remove dedup line")
        satisfied_rdd = (input_rdd.map(lambda x: remove_removal(x, args.duplicate_line_fraction, args.duplicate_line_character_faction, json_field))
                     .filter(lambda x: x[0]))
    else:
        print("use remove dedup doc")
        satisfied_rdd = input_rdd.filter(
            lambda x: check_removal(x, args.duplicate_line_fraction, args.duplicate_line_character_faction, json_field))


    satisfied_rdd.saveAsTextFile(good_output_path)
    unsatisfied_rdd = input_rdd.sample(False, 0.1).filter(
        lambda x: not check_removal(x, args.duplicate_line_fraction, args.duplicate_line_character_faction, json_field))
    unsatisfied_rdd.saveAsTextFile(bad_output_path)
    print(f"输入总文档数:{input_rdd.count()}, 留存文档总数:{satisfied_rdd.count()}, 留存率:{satisfied_rdd.count() / input_rdd.count() * 100:.2f}%")
    # 关闭Spark
    sc.stop()