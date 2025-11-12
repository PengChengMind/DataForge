#coding: utf-8

from __future__ import unicode_literals
import glob
import random
import json
import multiprocessing
import time
import pyarrow.parquet as pq
import pandas as pd
import json

def format_file(input_file_name, save_path):
    try:
        print(input_file_name)
        pre_file_name = input_file_name.split("/")[-1].split(".parquet")[0]
        file_timestamp = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
        out_file_name = save_path + pre_file_name + "_" + file_timestamp + ".json"
        print(out_file_name)
        table = pq.read_table(input_file_name)
        data = table.to_pandas()
        json_data = data.to_json(orient='records' ,force_ascii=False, lines=True).replace('"content":', '"text":')
        with open(out_file_name, 'w') as f:
            f.write(json_data)
    except Exception as e:
        print(e)
        traceback.print_exc()


if __name__ == "__main__":
    raw_data_root_path = "/usr/local/service/PC_Mind_200B_train_dataset/train_merged_data/en_dataset/"
    process_files = glob.glob(raw_data_root_path + "*.parquet")

    save_data_path = "/usr/local/service/PC_Mind_200B_train_dataset/train_merged_data/en_dataset/filter_files/"
    save_data_paths = [save_data_path for i in range(len(process_files))]
    print(len(process_files))
    random.shuffle(process_files)

    num_processes = 2
    pool = multiprocessing.Pool(processes=num_processes)
    for file_name, save_data_path in zip(process_files,save_data_paths):
        pool.apply_async(format_file, (file_name, save_data_path))
    pool.close()
    pool.join()
