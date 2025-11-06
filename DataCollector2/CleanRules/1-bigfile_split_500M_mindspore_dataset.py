#coding: utf-8

import glob
# import jsonlines
import os
import json
import codecs

# 将一个大文件切分成指定大小的文件
def bigfile_split(input_file_name,save_path,file_size):
    data_set_name = input_file_name.split("/")[-1].split(".")[0]
    input_file = codecs.open(input_file_name, 'r+', encoding='utf-8')
    save_num = 1
    save_file = codecs.open(save_path + data_set_name + "-part-" + str(save_num) + '.json', 'w', encoding='utf-8')
    while True:
        line = input_file.readline()
        if not line:
            break
        if len(line) == 0:
            continue
        save_file.write(line)
        # 保存为指定大小的文件
        if os.path.getsize(save_path + data_set_name + "-part-" + str(save_num) + '.json') > file_size:
            save_num += 1
            save_file = codecs.open(save_path + data_set_name + "-part-" + str(save_num) + '.json', 'w', encoding='utf-8')


root_path="/mgData1/zhangjiayuan/en_dataset_access/init/pile_RedPajama_arxiv/"
save_path="/mgData1/zhangjiayuan/en_dataset_access/process/process_init/pile_RedPajama_arxiv/"
data_set_name = ["pile_RedPajama_arxiv.json",

                 ]
split_file_size = 500000000 #500M
for data_name in data_set_name:
    input_file_name = root_path + data_name
    bigfile_split(input_file_name,save_path,split_file_size)
