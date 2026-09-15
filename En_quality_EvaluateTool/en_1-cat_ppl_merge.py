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
def quality_ppl_rank(remote_file_list, pid, origin_dir, final_dir, ratio):
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

        save_num = int(len(data)*ratio)
        data_new = sorted(data, key=lambda x: x['perplexity'])
        
        # 保存处理后的数据到新文件
        local_save_file = f"{final_dir}/{last_name}"
        last_save_file = f"/cache/last/{last_name}"
        with open(local_save_file, 'w', encoding='utf-8') as f:
            with open(last_save_file, 'w', encoding='utf-8') as f1:
                for idx1, item in enumerate(data_new):
                    if idx1 <= save_num:
                        f.write(json.dumps(item, ensure_ascii=False) + '\n')
                    else:
                        f1.write(json.dumps(item, ensure_ascii=False) + '\n')

        e1 = time.time()
        print(f"process file {idx}/{total_number}, size: {os.stat(local_file).st_size / 1024/1024} MB, elapsed time: {e1 - b1}s", flush=True)

def get_remote_files(input_obs_dir, rank, task_num):
    file_list = mox.file.list_directory(input_obs_dir, recursive=True)
    selected_files = []
    for filename in file_list:
        if filename.endswith(f".json"):
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
    parser.add_argument('--ratio', type=float, default=0.7)
    parser.add_argument('--output_obs_dir', type=str, default=None)
    args = parser.parse_args()

    rank_id = int(os.getenv('RANK_ID', '0'))
    if rank_id % 8 == 0:

        final_dir = f"/cache/{args.data_name}"
        origin_dir = f"/cache/origin_{args.rank}"
        last_dir = f"/cache/last"
        if not os.path.exists(final_dir):
            os.mkdir(final_dir)
        if not os.path.exists(origin_dir):
            os.mkdir(origin_dir)
        if not os.path.exists(last_dir):
            os.mkdir(last_dir)

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
            p = Process(target=quality_ppl_rank, args=(process_file_list[i], i, origin_dir, final_dir, args.ratio))
            process_list.append(p)
            p.start()
        for p in process_list:
            p.join()
        end = time.time()
        print(f"read time: {end - begin} s", flush=True)

        line = f"cat {final_dir}/*.json >>/cache/{args.data_name}.json"
        os.system(line)

        save_name = f"{args.data_name}-last_{(1-args.ratio):.2f}.json"
        line = f"cat {last_dir}/*.json >>/cache/{save_name}"
        os.system(line)
        cat_end = time.time()
        print(f"cat time: {cat_end - end} s", flush=True)

        mox.file.copy_parallel(f"/cache/{args.data_name}.json", os.path.join(args.output_obs_dir, "reserved", args.data_name+".json"))
        mox.file.copy_parallel(f"/cache/{save_name}", os.path.join(args.output_obs_dir, "discarded", save_name))
        print(f"upload time: {time.time() - cat_end} s", flush=True)
