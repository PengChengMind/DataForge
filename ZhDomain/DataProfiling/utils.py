# coding: UTF-8
import torch
from tqdm import tqdm
import time
from datetime import timedelta
import json
import random
from torch.utils.data import Dataset
import glob
import multiprocessing
import os

PAD, CLS = '[PAD]', '[CLS]'  # padding符号, bert中综合信息符号
SEP = '[SEP]'  # padding符号, bert中综合信息符号


def build_dataset(config):

    def load_dataset(path, pad_size=512):
        contents = []
        with open(path, 'r', errors='ignore') as f:
           # data_info = json.load(f)
            for line in tqdm(f.readlines()):
                lin = line.strip()
                if not lin:
                    continue
                try:
                    lin = eval(lin)
                except:
                    continue
                input_str=lin["question"]
                #output_str=lin["output"]
                #if lin["type"]==15 or lin["type"]==16 or lin["type"]==17:
                #    label=1
                #else:
                #    label=0
                label=int(lin["label"])
                content=input_str
                #response=output_str
                token_1 = config.tokenizer.tokenize(content)
                #token_2 = config.tokenizer.tokenize(response)
                token = [CLS] + token_1# + [SEP] +token_2
                seq_len = len(token)
                mask = []
                token_ids = config.tokenizer.convert_tokens_to_ids(token)

                if pad_size:
                    if len(token) < pad_size:
                        mask = [1] * len(token_ids) + [0] * (pad_size - len(token))
                        token_ids += ([0] * (pad_size - len(token)))
                    else:
                        mask = [1] * pad_size
                        token_ids = token_ids[:pad_size]
                        seq_len = pad_size
                contents.append((token_ids, int(label), seq_len, mask))
                random.shuffle(contents)
        return contents
    train = load_dataset(config.train_path, config.pad_size)
    dev = load_dataset(config.dev_path, config.pad_size)
    test = load_dataset(config.test_path, config.pad_size)
    return train, dev, test


class DatasetIterater(object):
    def __init__(self, batches, batch_size, device):
        self.batch_size = batch_size
        self.batches = batches
        self.n_batches = len(batches) // batch_size
        self.residue = False  # 记录batch数量是否为整数
        if len(batches) % self.n_batches != 0:
            self.residue = True
        self.index = 0
        self.device = device

    def _to_tensor(self, datas):
        x = torch.LongTensor([_[0] for _ in datas]).to(self.device)
        y = torch.LongTensor([_[1] for _ in datas]).to(self.device)

        # pad前的长度(超过pad_size的设为pad_size)
        seq_len = torch.LongTensor([_[2] for _ in datas]).to(self.device)
        mask = torch.LongTensor([_[3] for _ in datas]).to(self.device)
        return (x, seq_len, mask), y

    def __next__(self):
        if self.residue and self.index == self.n_batches:
            batches = self.batches[self.index * self.batch_size: len(self.batches)]
            self.index += 1
            batches = self._to_tensor(batches)
            return batches

        elif self.index >= self.n_batches:
            self.index = 0
            raise StopIteration
        else:
            batches = self.batches[self.index * self.batch_size: (self.index + 1) * self.batch_size]
            self.index += 1
            batches = self._to_tensor(batches)
            return batches

    def __iter__(self):
        return self

    def __len__(self):
        if self.residue:
            return self.n_batches + 1
        else:
            return self.n_batches


def build_iterator(dataset, config):
    iter = DatasetIterater(dataset, config.batch_size, config.device)
    return iter


def get_time_dif(start_time):
    """获取已使用时间"""
    end_time = time.time()
    time_dif = end_time - start_time
    return timedelta(seconds=int(round(time_dif)))

class InferDatasetIterater(object):
    def __init__(self, batches, batch_size, device):
        self.batch_size = batch_size
        self.batches = list(batches)
        self.n_batches = len(batches) // batch_size
        self.residue = False  # 记录batch数量是否为整数
        if len(batches) % self.n_batches != 0:
            self.residue = True
        self.index = 0
        self.device = device

    def _to_tensor(self, datas):
        x = torch.LongTensor([_[0] for _ in datas]).to(self.device)
        # y = torch.LongTensor([_[1] for _ in datas]).to(self.device)
        y = [_[1] for _ in datas]

        # pad前的长度(超过pad_size的设为pad_size)
        seq_len = torch.LongTensor([_[2] for _ in datas]).to(self.device)
        mask = torch.LongTensor([_[3] for _ in datas]).to(self.device)
        return (x, seq_len, mask), y

    def __next__(self):
        if self.residue and self.index == self.n_batches:
            batches = self.batches[self.index * self.batch_size: len(self.batches)]
            self.index += 1
            batches = self._to_tensor(batches)
            return batches

        elif self.index >= self.n_batches:
            self.index = 0
            raise StopIteration
        else:
            batches = self.batches[self.index * self.batch_size: (self.index + 1) * self.batch_size]
            self.index += 1
            batches = self._to_tensor(batches)
            return batches

    def __iter__(self):
        return self

    def __len__(self):
        if self.residue:
            return self.n_batches + 1
        else:
            return self.n_batches

class InferDatasetIterater_2(object):
    def __init__(self, batches, batch_size, device):
        self.batch_size = batch_size
        self.batches = batches
        self.index = 0
        self.device = device

    def _to_tensor(self, datas):
        x = torch.LongTensor([_[0] for _ in datas]).to(self.device)
        # y = torch.LongTensor([_[1] for _ in datas]).to(self.device)
        y = [_[1] for _ in datas]

        # pad前的长度(超过pad_size的设为pad_size)
        seq_len = torch.LongTensor([_[2] for _ in datas]).to(self.device)
        mask = torch.LongTensor([_[3] for _ in datas]).to(self.device)
        return (x, seq_len, mask), y

    def __next__(self):
        current_batch_data = []
        while len(current_batch_data)<self.batch_size:
            try:
                current_batch_data.append(next(self.batches))
                self.index += 1
            except StopIteration:
                raise StopIteration
        current_batch_data = self._to_tensor(current_batch_data)
        return current_batch_data

    def __iter__(self):
        return self

    def __len__(self):
        return self.index


def build_infer_dataset(config,data_file_name):
    def load_dataset(path, pad_size=512):
        contents = []
        with open(path, 'r', errors='ignore') as f:
            data_lines = f.readlines()
            for line in data_lines:
                line_json = json.loads(line)
                content=line_json["text"]
                token_1 = config.tokenizer.tokenize(content)
                #token_2 = config.tokenizer.tokenize(response)
                token = [CLS] + token_1 # + [SEP] +token_2
                seq_len = len(token)
                mask = []
                token_ids = config.tokenizer.convert_tokens_to_ids(token)

                if pad_size:
                    if len(token) < pad_size:
                        mask = [1] * len(token_ids) + [0] * (pad_size - len(token))
                        token_ids += ([0] * (pad_size - len(token)))
                    else:
                        mask = [1] * pad_size
                        token_ids = token_ids[:pad_size]
                        seq_len = pad_size
                contents.append((token_ids, line_json, seq_len, mask))
                # print(line_json["text"][:10])
        # random.shuffle(contents)
        return contents
    test_dataset = load_dataset(data_file_name, config.pad_size)
    test_dataset_iter = InferDatasetIterater(test_dataset, config.batch_size, config.device)
    return test_dataset_iter


class dataEncoder(object):
    def __init__(self, config,pad_size=512):
        self.config = config
        self.pad_size = pad_size

    def initializer(self):
        # Use Encoder class as a container for global data
        dataEncoder.tokenizer = self.config.tokenizer

    def encode(self, line):
        # print(line)
        line_json = json.loads(line)
        content = line_json["text"]
        token_1 = dataEncoder.tokenizer.tokenize(content)
        # token_2 = config.tokenizer.tokenize(response)
        token = [CLS] + token_1  # + [SEP] +token_2
        seq_len = len(token)
        mask = []
        token_ids = dataEncoder.tokenizer.convert_tokens_to_ids(token)

        if self.pad_size:
            if len(token) < self.pad_size:
                mask = [1] * len(token_ids) + [0] * (self.pad_size - len(token))
                token_ids += ([0] * (self.pad_size - len(token)))
            else:
                mask = [1] * self.pad_size
                token_ids = token_ids[:self.pad_size]
                seq_len = self.pad_size
        return token_ids, line_json, seq_len, mask

def build_infer_dataset_multiprocess(config,data_file_name):
    def load_dataset(path, pad_size=512):
        fin = open(path, 'r', encoding='utf-8')
        encoder = dataEncoder(config, pad_size=pad_size)
        workers_num = 100
        start_t = time.time()
        pool = multiprocessing.Pool(workers_num, initializer=encoder.initializer)
        encoded_docs = pool.imap(encoder.encode, fin, 25)

        # print(f"---data_file_name:{data_file_name}----multiprocessing time:{time.time() - start_t} s------")
        # contents = []
        # for i, (token_ids, line_json, seq_len, mask) in enumerate(encoded_docs, start=1):
        #     contents.append((token_ids, line_json, seq_len, mask))
        # # random.shuffle(contents)
        return encoded_docs
    start_t = time.time()
    test_dataset = load_dataset(data_file_name, config.pad_size)
    print(f"---data_file_name:{data_file_name}---size:{os.path.getsize(data_file_name)}-----load time:{time.time() - start_t} s------")
    test_dataset_iter = InferDatasetIterater_2(test_dataset, config.batch_size, config.device)
    print(f"---data_file_name:{data_file_name}---size:{os.path.getsize(data_file_name)}-----Iterater time:{time.time() - start_t} s------")
    return test_dataset_iter
