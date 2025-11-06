import glob
# import jsonlines
import os
import json
import random
import argparse

def data_sample_document_level(data_file_name,save_file_name,sample_length,sample_rate):
    input_file = open(data_file_name, 'r+', encoding='utf-8')
    save_file = open(save_file_name, 'w', encoding='utf-8')

    # 如果需要抽样多倍数据,
    if sample_rate >1:
        # sample_rate = 1
        int_value = int(sample_rate)
        for i in range(int_value):
            while True:
                line = input_file.readline()
                if not line:
                    break
                if line == '':
                    continue
                save_file.write(line)

        sample_rate = sample_rate-int_value

    #剩余不到一倍的部分数据随机抽样
    document_pool_list = []
    while True:
        line = input_file.readline()
        if not line:
            break
        if line == '':
            continue

        if len(document_pool_list)==sample_length:
            sample_result_list = random.sample(document_pool_list, int(sample_rate*sample_length))
            for item in sample_result_list:
                save_file.write(item)
            document_pool_list = []
        else:
            document_pool_list.append(line)

#     从最后一个list中采样,如果一个list少于20个document，就丢掉不要
    if len(document_pool_list)>20:
        sample_result_list = random.sample(document_pool_list, int(sample_rate * len(document_pool_list)))
        for item in sample_result_list:
            save_file.write(item)


def main(args):

    input_data_file = args.input_data_path_and_sample_size.split("@@@")[-1]
    save_data_path = args.output_save_path

    data_file_name = input_data_file.split("/")[-1]
    input_data_disk_size = os.path.getsize(input_data_file)
    print(f"---dataset:{data_file_name}----disk size:{input_data_disk_size}---")

    # 定义需要在多大document内进行打乱及采样
    sample_length = 500000

    sample_disk_size = float(args.input_data_path_and_sample_size.split("@@@")[0])
    print(data_file_name, "-----sample start")
    sample_rate = sample_disk_size * 1000000000 / input_data_disk_size
    if sample_rate == 0:
        print(f"---sampled data disk size:0---")
        return 0
    data_sample_document_level(input_data_file,
                               save_data_path + data_file_name,
                               sample_length,
                               sample_rate)
    total_size = os.path.getsize(save_data_path + data_file_name)
    print(f"---sampled data disk size:{total_size}---")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_data_path_and_sample_size', type=str, required=True,
                       help='input data path and sample size')
    parser.add_argument('--output_save_path', type=str, required=True,
                       help='output_save_path')
    args = parser.parse_args()

    main(args)

