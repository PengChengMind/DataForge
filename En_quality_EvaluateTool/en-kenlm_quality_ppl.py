# utf-8
"""
transform dataset to mindrecord.
"""
import argparse
import glob
import json
import os
import random
from multiprocessing import Pool, current_process, Process
import numpy as np
import moxing as mox
import time
import mindspore.communication.management as D
import multiprocessing
import sentencepiece as spm
import kenlm
import pandas as pd
import math
import time
import json
import os

import sys
# print(os.system(("pip install transformers")))

sys.setrecursionlimit(100000)


def chunks(lst, n):
    """ yield n sized chunks from list"""
    temp = []
    for item in lst:
        while len(item) > (n - len(temp)):
            temp = temp + item[:(n-len(temp))]
            yield temp
            temp = []
            item = item[(n-len(temp)):]
        temp = temp + item
    yield temp

def padding_eot(chunk, PAD, seq_length):
    pad = [PAD] * (seq_length - len(chunk))
    chunk.extend(pad)
    return chunk

# 将一个list尽量均分成n份，限制len(list)==n，份数大于原list内元素个数则分配空list[]
def divideIntoNstrand(listTemp, n):
    twoList = [[] for i in range(n)]
    for i,e in enumerate(listTemp):
        twoList[i%n].append(e)
    return twoList

def package_file(it):
    """ package multiple files"""
    for batch in it:
        yield batch

def compute_perplexity(text, sp, lm):
    tokens = sp.encode(text, out_type=str)
    tokenized_text = ' '.join(tokens)
    logprob_sum = 0.0
    num_tokens = len(tokens)
    for word, logprob, length in lm.full_scores(tokenized_text):
        logprob_sum += logprob
    # 将 logprob_sum 从自然对数转换为以 2 为底的对数
    logprob_sum /= math.log(2)
    perplexity = pow(2, - (logprob_sum / num_tokens))
    return perplexity

def assign_bucket(perplexity, pp_head, pp_tail):
    if perplexity < pp_head:
        return 'head'
    elif perplexity < pp_tail:
        return 'middle'
    else:
        return 'tail'

def quality_ppl_rank(remote_file_list, pid, sp, lm, origin_dir, final_dir):
    content = []
    total_number = len(remote_file_list)
    for idx, remote_file in enumerate(remote_file_list):
        b1 = time.time()
        last_name = remote_file.split("/")[-1]
        
        local_file = f"{origin_dir}/{pid}_{last_name}"
        mox.file.copy_parallel(remote_file, local_file)

        # 读取 JSON 文件
        with open(local_file, 'r', encoding='utf-8') as f:
            data = [json.loads(line) for line in f]
        data_new = []
        for info_one in data:
            try:
                para = info_one
                text = para['text']
                perplexity = compute_perplexity(text, sp, lm)
                bucket = assign_bucket(perplexity, pp_head, pp_tail)
                para['perplexity'] = perplexity
                para['bucket'] = bucket
                data_new.append(para)

            except Exception as error:
                print(error)

        # 保存处理后的数据到新文件
        local_save_file = f"{final_dir}/{last_name}"
        with open(local_save_file, 'w', encoding='utf-8') as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')

        e1 = time.time()
        print(f"process file {idx}/{total_number}, size: {os.stat(local_file).st_size / 1024/1024} MB, elapsed time: {e1 - b1}s", flush=True)

def get_remote_files(input_obs_dir, rank, task_num):
    file_list = mox.file.list_directory(input_obs_dir, recursive=True)
    selected_files = []
    for filename in file_list:
        for k in range(64):
            if filename.endswith(f"_{rank+task_num*k}.json"):
                selected_files.append(os.path.join(input_obs_dir, filename))
    return selected_files

def get_total_size(file_list):
    total_size = 0
    for file in file_list:
        info = mox.file.stat(file)
        total_size += info.st_size
    return total_size

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_obs_dir', type=str,
                        default="obs://sunzhb/scaling_law/dataset/2024_naohai7b_continue_datasets_split_512_v2")
    parser.add_argument('--data_name', type=str, default=None)
    parser.add_argument('--task_num', type=int, default=0)
    parser.add_argument('--rank', type=int, default=0)
    parser.add_argument('--num_process', type=int, default=24)
    parser.add_argument('--output_obs_dir', type=str, default=None)
    args = parser.parse_args()

    final_dir = f"/cache/{args.data_name}"
    origin_dir = f"/cache/origin_{args.rank}"
    if not os.path.exists(final_dir):
        os.mkdir(final_dir)
    if not os.path.exists(origin_dir):
        os.mkdir(origin_dir)

    # 加载分词器
    sp = spm.SentencePieceProcessor()
    sp.load('/home/ma-user/modelarts/user-job-dir/cc/en.sp.model')

    # 加载语言模型
    lm = kenlm.Model('/home/ma-user/modelarts/user-job-dir/cc/en.arpa.bin')

    # 读取 cutoff.csv
    cutoff_df = pd.read_csv('/home/ma-user/modelarts/user-job-dir/cc/cutoff.csv', index_col=0, encoding='utf-8')
    en_cutoffs = cutoff_df['en']

    # 定义百分位数
    percentile_head = 30
    percentile_tail = 60

    # 获取困惑度阈值
    pp_head = en_cutoffs.loc[percentile_head]
    pp_tail = en_cutoffs.loc[percentile_tail]

    obs_file_dir = os.path.join(args.input_obs_dir, args.data_name)
    remote_file_list = get_remote_files(obs_file_dir, args.rank, args.task_num)
    total_number = len(remote_file_list)
    total_size = get_total_size(remote_file_list)
    print(f'file number: {total_number}, total size: {total_size/1024/1024} MB')
    
    file_size_list = []
    for file in remote_file_list:
        size = mox.file.stat(file).st_size
        file_size_list.append((file, size))
    sorted_file_list = sorted(file_size_list, key=lambda x:x[1], reverse=True)
    #sorted_file_list = sorted_file_list
    
    avg_file_size = total_size / args.num_process
    process_file_list = [[] for _ in range(args.num_process)]
    process_size_list = [0] * args.num_process
    pid = 0
    assert len(sorted_file_list) >= args.num_process
    for i in range(args.num_process):
        item = sorted_file_list[args.num_process - i - 1]
        process_file_list[i].append(item[0])
        process_size_list[i] += item[1]
        
    for item in sorted_file_list[args.num_process:]:
        if process_size_list[pid] > avg_file_size:
            pid += 1
        process_file_list[pid].append(item[0])
        process_size_list[pid] += item[1]
    whole_size = 0
    for pid in range(args.num_process):
        print(f"pid: {pid}, size: {process_size_list[pid] / 1024 / 1024} MB")
        whole_size += process_size_list[pid]
    print(f"whole size: {whole_size/1024/1024} MB")

    begin = time.time()
    process_list = []
    for i in range(args.num_process):
        p = Process(target=quality_ppl_rank, args=(process_file_list[i], i, sp, lm, origin_dir, final_dir))
        process_list.append(p)
        p.start() 
    for p in process_list:
        p.join()
    end = time.time()
    print(f"read time: {end - begin} s", flush=True)
    
    mox.file.copy_parallel(final_dir, os.path.join(args.output_obs_dir, args.data_name))



