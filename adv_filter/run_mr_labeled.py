import torch
import jieba
import pandas as pd
from torch.utils.data import Dataset 
import os
from torchtext.data.functional import to_map_style_dataset
from torch.utils.data import DataLoader
import torch.nn as nn
import json
from  datetime import datetime

import argparse
import torch.multiprocessing
from concurrent.futures import ThreadPoolExecutor
import threading 
import numpy

# p_lock = torch.multiprocessing.Lock()
# t_lock = threading.Lock()
torch.multiprocessing.set_start_method('spawn',force=True)
device = torch.device("cpu" if torch.cuda.is_available() else "cpu")
BATCH_SIZE = 64 # batch size for training

def parse_parameters():
    parser = argparse.ArgumentParser(description='Accepts -config (config file) and -d (data source) arguments, filter advertisements in data source.')
    parser.add_argument('-d', '--data', help='specify the directory or file path. ',default='/home/zlj/code/pcl/template/data/test_data/')
    parser.add_argument('-o', '--output', default='/home/zlj/code/pcl/template/output', help='specify the directory or file path.')
    args = parser.parse_args()
    return args

def is_string_in_file(target_string, file_path):  
    with open(file_path, 'r') as file:
        return target_string in file.read()

# 获取指定路径下的所有文件
def get_files(path):
    files = os.listdir(path)
    return files

# 将字符串格式化成json字符串
def json_format(string):
    return json.dumps(eval(string),ensure_ascii=False)

# 读取文件中的数据,处理掉一些没用的数据
def read_data(file_path):
    print("------------------------")
    print("文件名：{}",file_path)
    data_filter = []
    with open(file_path, 'r',encoding='utf-8') as file:
        data = file.read().splitlines()
        for line in data:
            if line.find("text") != -1: #过滤掉非text的文本
                data_filter.append(line)
    return data_filter

class textDataset(Dataset):
    def __init__(self, csv_path):
        super().__init__()
        self.install_data = pd.read_csv(csv_path, encoding='utf-8', sep = '###', header = None)


    def __len__(self):
        return len(self.install_data)

    def __getitem__(self, idx):
        input_data = pd.DataFrame([])
        label = pd.DataFrame([])
        input_data = self.install_data.iloc[idx, 0]
        label = self.install_data.iloc[idx, 1]
        return label, input_data


class inputtextDataset(Dataset):
    def __init__(self, text_list):
        super().__init__()
        self.install_data = text_list

    def __len__(self):
        return len(self.install_data)

    def __getitem__(self, idx):
        input_data = self.install_data[idx]
        return input_data


tokenizer = jieba.lcut

high_low_en_path_json = 'data/word_dict.json'

def get_vocab_content(path_json):
    with open(path_json, 'r', encoding = 'utf-8') as fr:
        vocab_content = eval(json.load(fr))
    return vocab_content
high_low_en_vocab_content = get_vocab_content(high_low_en_path_json)

def high_low_en_collate_batch(batch):
    def vocab(text_list):
        ind_list = []
        for elem in text_list:
            try:
                ind_list.append(int(high_low_en_vocab_content[elem]))
            except:
                ind_list.append(int(0))
        return ind_list 
    text_pipeline = lambda x: vocab(tokenizer(x))    
    
    text_list, offsets = [], [0]
    for  _text in batch:
         processed_text = torch.tensor(text_pipeline(_text), dtype=torch.int64)
         text_list.append(processed_text)
         offsets.append(processed_text.size(0))
    offsets = torch.tensor(offsets[:-1]).cumsum(dim=0)
    text_list = torch.cat(text_list)
    return text_list.to(device), offsets.to(device)

class TextClassificationModel(nn.Module):
    def __init__(self, vocab_size, embed_dim, num_class):
        super(TextClassificationModel, self).__init__()
        self.embedding = nn.EmbeddingBag(vocab_size, embed_dim, sparse=True)
        self.fc = nn.Linear(embed_dim, num_class)
        self.init_weights()

    def init_weights(self):
        initrange = 0.5
        self.embedding.weight.data.uniform_(-initrange, initrange)
        self.fc.weight.data.uniform_(-initrange, initrange)
        self.fc.bias.data.zero_()

    def forward(self, text, offsets):
        embedded = self.embedding(text, offsets)
        return self.fc(embedded)


num_class = 2
high_low_en_vocab_size = len(high_low_en_vocab_content)
emsize = 64

# simplenet
high_low_en_model = TextClassificationModel(high_low_en_vocab_size, emsize, num_class).to(device)
high_low_en_model.load_state_dict(torch.load('/home/zlj/code/pcl/template/model/model.pt', map_location=torch.device('cuda')))
print("high_low_en模型总参数:", sum(p.numel() for p in high_low_en_model.parameters()))
high_low_en_model.eval()

def high_low_en(datalist):
    # 高质量文本的结果
    high_text_results = []
    # 低质量文本的结果
    low_text_results = []
    # 原数据基础上增加标识的结果
    addflag_results = []
    # 原始数据，用来对比清洗数据量
    raw_data = []
    for i in range(len(datalist)):
        if datalist[i].find("text") == -1: #过滤掉非text的文本
            continue
        text_json = json.loads(datalist[i])
        test_list = [text_json["text"].replace('\\n','').replace('\n','')]
        test_iter = inputtextDataset(test_list)
        test_dataset = to_map_style_dataset(test_iter)
        test_dataloader = DataLoader(test_dataset, batch_size=BATCH_SIZE,
                                shuffle=False, collate_fn=high_low_en_collate_batch)

        predicted_list = []
        with torch.no_grad():
            for idx, (text, offsets) in enumerate(test_dataloader):
                predicted_label = high_low_en_model(text, offsets)
                predicted_list += predicted_label.argmax(1).tolist()      
        if 1 in predicted_list:
            high_text_results.append(datalist[i])
        elif 0 in predicted_list:
            low_text_results.append(datalist[i])

        text_json["part_status_id"] = predicted_list
        addflag_results.append(json.dumps(text_json,ensure_ascii=False))

        raw_data.append(datalist[i])
    return high_text_results,low_text_results,addflag_results,raw_data


def evaluate(dataloader):
    model.eval()
    total_acc, total_count = 0, 0

    with torch.no_grad():
        for idx, (label, text, offsets) in enumerate(dataloader):
            predicted_label = model(text, offsets)
            loss = criterion(predicted_label, label)
            total_acc += (predicted_label.argmax(1) == label).sum().item()
            total_count += label.size(0)
        print('Test accuracy is:', total_acc / total_count)
    return total_acc/total_count

def process_filter_file(parameter):
    start_datetime = datetime.now()
    output_base_path, data_path, file_name = parameter
    data_dir_name = os.path.basename(data_path.rstrip('/'))
#    output_path = os.path.join(output_base_path, data_dir_name, file_name)
    file_path = os.path.join(data_path, file_name)

    high_text_path = os.path.join(output_base_path, data_dir_name, "high_text", file_name)
    low_text_path = os.path.join(output_base_path, data_dir_name,"low_text", file_name)
    addflag_path = os.path.join(output_base_path, data_dir_name,"add_flag", file_name)
    rawdata_path = os.path.join(output_base_path, data_dir_name,"raw_data", file_name)

    #判断文件是否已处理过
    if os.path.exists(rawdata_path):
        print(file_path+"已处理")
        return   
    data = read_data(file_path)
    
    # 质量分类
    high_text_results,low_text_results,addflag_results,raw_data = high_low_en(data)
    # if not len(addflag_results) == 0:
    #     print(file_name,"high_low_en_results is None")
    #     return
    print("high_low_en is ok")
    if  high_text_results:
        output_results(high_text_path, high_text_results)
    if low_text_results:
        output_results(low_text_path,low_text_results)
    output_results(addflag_path,addflag_results)
    output_results(rawdata_path,raw_data)

    
    print("File processing time:",datetime.now()-start_datetime)
    # p_lock.acquire()
    # t_lock.acquire()
    # with open('record.txt','a+') as f:
    #     f.write(os.path.join(data_path, file_name) + '\n')
    # t_lock.release()
    # p_lock.release()      
    
# 将结果输出到文件
def output_results(output_path,results):
    # 判断文件夹是否存在
    folder_name = os.path.dirname(output_path)
    if not os.path.exists(folder_name):
        # 如果文件夹不存在，则创建它
        os.makedirs(folder_name)
    print(output_path)
    with open(output_path, 'w',encoding='utf-8') as f:
        for result in results:
            f.write(str(result) + "\n")
    
def thread_executor(parameters):
    # 每个进程创建6个线程，并发处理文本,超出6个线程可能会造成资源不足（系统配置导致？）
    with ThreadPoolExecutor(max_workers=4) as executor:  
        for parameter in parameters:  
            executor.submit(process_filter_file, parameter)    

if __name__ == '__main__':
    total_start_datetime = datetime.now()
    args = parse_parameters()
    data_path = args.data
    output_base_path = args.output
    files = get_files(data_path)
    # 将所有文本分成cpu核心数量等同份数
    parameters_list = numpy.array_split([(output_base_path, data_path, file_name) for file_name in files ],os.cpu_count())

    # 根据cpu核心数创建进程数量，每个cpu核处理一份数据
    p_pool = torch.multiprocessing.Pool(os.cpu_count())
    _ = p_pool.map(thread_executor, parameters_list)
    p_pool.close()
    p_pool.join()
    print("total run time is ", datetime.now() - total_start_datetime)
