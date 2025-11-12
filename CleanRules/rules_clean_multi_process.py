#coding: utf-8

from __future__ import unicode_literals
import json
import glob
import multiprocessing
import re
import random
import string
import time
from bs4 import BeautifulSoup
from urlextract import URLExtract
import collections
import argparse
import re
import functools
import codecs
import traceback
import zhconv

_SENTENCE_TOKENIZER = None

def is_bad_text(text,title):
    # print(text+"\n\n")
    is_delete = False

    #长度为0的
    if len(text)<1:
        is_delete = True
        return is_delete

    # 统计标点符号数量,过滤没有标点符号的数据
    prog_1 = re.compile(r"[\，\。\？\！\| \… \. \: \（ \） \、\"]", re.S)
    res1 = re.findall(prog_1, text)
    if 1.0 * len(res1) / len(text) < 0.005:
        is_delete = True
        return is_delete

    #过滤报业数据中的其他国家的新闻数据：统计中文字符比例小于50%的数据过滤掉
    pattern = re.compile(u'[\u4e00-\u9fa5]')
    result = pattern.findall(text)
    count = len(result)
    if 1.0*count/len(text) <0.5:
        is_delete = True
        return is_delete

    #规则：内容字符数少于50，或者新闻内容与标题相同
    try:
        if len(text) < 50 or text == title:
            is_delete = True
            return is_delete
    except:
        i=1

    return is_delete

def readLineContaining_fuhao(line):
    is_delete = False
    punctuation = '|_>.·*'
    if line == '\n':
        is_delete = True
        return is_delete

    line = re.sub('\s|\t','',line)
    if line:
        punc_count = sum([line.count(char) for char in punctuation])
        rate = 1.0 * punc_count / len(line)
        if rate > 0.5:
            is_delete = True
            return is_delete

    else:
        is_delete = True
        return is_delete
    return is_delete


def is_url_regex(string):
    """
    判断字符串是否为URL的正则表达式匹配方法
    """
    regex = (
        r'^https?:\/\/(?:www\.)?'
        r'(?:(?:[A-Z\d][A-Z\d-]{0,61}[A-Z\d]\.)|[A-Z\d]\.)'
        r'+[A-Z]{2,6}(?::\d+)?'
        r'(?:\/[-a-zA-Z\d%_.~+]*)*'
        r'(?:\?[;&a-zA-Z\d%_.~+=-]*)?'
        r'(?:#[-a-zA-Z\d_]*)?$')
    return re.match(regex, string, re.IGNORECASE)


def traditional2simplified(key_str):
    """ 将key_str中的繁体字转为简体字

    :param key_str: str, 需要做简繁体转换的字符串
    :return: str, key_str对应的简体字
    """
    return zhconv.convert(key_str, 'zh-hans')

def html2plaintext(html):
    soup = BeautifulSoup(html, features="html.parser")

    try:
        if soup.div["id"]=="copyright":
            return ""
    except:
        a=0
    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()  # rip it out
    divs = soup.find_all('div')
    for div in divs:
        for a in div.find_all('a'):
            a.replace_with_children()
    # get text
    text = soup.get_text()
    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)
    # print(text)
    return text

def rules_filter(data_dict):
    content = data_dict["text"]
    citation_regex = re.compile(r"\[\d*\]|\[edit\]|\[citation needed\]")
    content = citation_regex.sub("", content)

    try:
        content = html2plaintext(content)
    except Exception as e:
        pass

    # 连续特殊符号，则删除字符
    pattern_symbol_same = re.compile(r"[-]{4,}|[.]{4,}|[=]{4,}", re.S)
    content = re.sub(pattern_symbol_same, ' ', content)

    # 特殊符号且无含义，则只删除字符
    pattern_symbol_special = re.compile(r"[\¤\⌒\々\〓\▌\◇\▲\△\▪\★\◆\▼\●\▽\◁\☆\○]", re.S)
    content = re.sub(pattern_symbol_special, '', content)

    # 繁体转简体
    #content = traditional2simplified(content)
    #pattern_zh = re.compile(u'[\u4e00-\u9fa5]|[\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b]')
    #content = re.sub(pattern_zh, '', content)

    is_delete = False
    if len(content) == 0:
        is_delete = True
        return is_delete,content

    #标点符号过多或者过少
    # 统计标点符号数量,过滤没有标点符号的数据
    punctuation_string = string.punctuation
    punc_count = sum([content.count(char) for char in punctuation_string])
    if 1.0*(punc_count)/len(content) <0.005 or 1.0*(punc_count)/len(content) >0.5:
        is_delete = True
        return is_delete,content
    #print(f"punc_count:{len(punc_count)/len(content)}")
    #每行有太长的文字

    #中英数字比例过低
    pattern_2 = re.compile(u'[\u4e00-\u9fa5]')
    letters = re.findall('[a-zA-Z]', content)
    nums = re.findall(r'[0-9]+', content)
    res1 = pattern_2.findall(content)
    rate = 1.0 * (len(res1) + len(letters)+len(nums)) / len(content)
    if rate < 0.4:
        is_delete = True
        return is_delete, content

    # for line in lines:
    lines = content.splitlines()
    for index in range(len(lines)-1, -1, -1):
        line = lines[index].strip()
        # line has too long word
        for word in line.split():
            if len(word) > 1000:
                is_delete = True
                return is_delete,content
        # line has too many fuhao
        is_line_delete = readLineContaining_fuhao(line)
        if is_line_delete:
            del lines[index]
    content = '\n'.join(lines)

    # 过滤长度小于50的内容
    if len(content)<50:
        is_delete = True
        return is_delete,content

    return is_delete,content

def clean_process_onefile(input_file_name,save_path):
    try:
        print(input_file_name)
        input_file = codecs.open(input_file_name, 'r+', encoding='utf-8')
        file_name = input_file_name.split("/")[-1].split(".json")[0]
        file_timestamp =  time.strftime("%Y-%m-%d-%H-%M-%S",time.localtime())
        bad_text_name = save_path + file_name + "_" + file_timestamp + "_bad.json"
        remain_text_name = save_path + file_name + "_" + file_timestamp + "_clean.json"

        bad_save_file = codecs.open(bad_text_name, 'w', encoding='utf-8')
        remain_save_file = codecs.open(remain_text_name, 'w', encoding='utf-8')

        while True:
            line_text = input_file.readline()

            if not line_text:
                break
            if len(line_text) == 0:
                continue
            try:
                data_dict = json.loads(line_text)
            except Exception as e:
                bad_save_file.write(line_text + '\n')
                continue
            is_delete, text = rules_filter(data_dict)
            sample_dict = {}
            sample_dict["text"] = text
            if "file" in data_dict.keys():
                sample_dict["file"] = data_dict["file"]
            if "src" in data_dict.keys():
                sample_dict["src"] = data_dict["src"]
            if "chapter_title" in data_dict.keys():
                sample_dict["chapter_title"] = data_dict["chapter_title"]
            if "type" in data_dict.keys():
                sample_dict["type"] = data_dict["type"]


            if is_delete:
                bad_save_file.write(json.dumps(sample_dict, ensure_ascii=False) + '\n')
                continue
            # json.dump(sample_dict, save_file)
            remain_save_file.write(json.dumps(sample_dict, ensure_ascii=False) + '\n')

    except Exception as e:
        traceback.print_exc()


if __name__ == "__main__":
    raw_data_root_path = "/usr/local/service/PC_Mind_200B_train_dataset/train_merged_data/en_dataset/"
    process_files = glob.glob(raw_data_root_path + "*.json")

    save_data_path = "/usr/local/service/PC_Mind_200B_train_dataset/train_merged_data/en_dataset/filter_files/"
    save_data_paths = [save_data_path for i in range(len(process_files))]
    print(len(process_files))
    random.shuffle(process_files)

    num_processes = 100
    pool = multiprocessing.Pool(processes=num_processes)
    for file_name, save_data_path in zip(process_files,save_data_paths):
        # print(file_name)
        pool.apply_async(clean_process_onefile, (file_name, save_data_path))
    pool.close()
    pool.join()
