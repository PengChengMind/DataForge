from http.client import HTTPException
import torch
import jieba
import pandas as pd
from torch.utils.data import Dataset 
import os
from torch.utils.data.dataset import random_split
from torchtext.data.functional import to_map_style_dataset
from torchtext.data.utils import get_tokenizer
from torchtext.vocab import build_vocab_from_iterator
from torch.utils.data import DataLoader
import time
import torch.nn as nn
from flask import Flask,jsonify,request
import logging
import json
import torch.nn.functional as F


# 配置日志
#logging.basicConfig(filename='log/flask.log', level=logging.DEBUG)

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

path_json = 'data/word_dict.json'

with open(path_json, 'r', encoding = 'utf-8') as fr:
    vocab_content = eval(json.load(fr))

def vocab(text_list):
    ind_list = []
    for elem in text_list:
        try:
            ind_list.append(int(vocab_content[elem]))
        except:
            ind_list.append(int(0))
    return ind_list 

text_pipeline = lambda x: vocab(tokenizer(x))

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

def collate_batch(batch):
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
        return embedded, self.fc(embedded)


num_class = 2
vocab_size = len(vocab_content)
emsize = 64


model = TextClassificationModel(vocab_size, emsize, num_class).to(device)
model.load_state_dict(torch.load('model/model.pt'))

print("模型总参数:", sum(p.numel() for p in model.parameters()))
model.eval()

BATCH_SIZE = 64 # batch size for training


app = Flask(__name__)
@app.route('/adclass', methods=['post','get'])
def adclass():

    output = {    
         "text": [],
         "source_ids": [],
         "source_type": [],
         "platform": 0,
         "status_id": [],
         "status_embed": [],
         "status_prob": [],
         "hit_groupword": []
        }

    test_list = request.get_json().get('texts')
    test_iter = inputtextDataset(test_list)
    test_dataset = to_map_style_dataset(test_iter)
    test_dataloader = DataLoader(test_dataset, batch_size=BATCH_SIZE,
                            shuffle=False, collate_fn=collate_batch) 

    predicted_prob_list = []
    predicted_list = []
    embed_list = []
    with torch.no_grad():
        for idx, (text, offsets) in enumerate(test_dataloader):
            embedded, predicted_label = model(text, offsets)
            #predicted_list += predicted_label.argmax(1).cpu().tolist()

            predicted_prob_list = F.softmax(predicted_label, dim = -1).cpu().tolist()
            predicted_list += predicted_label.cpu().tolist()
            embed_list += embedded.cpu().tolist()

    output["status_id"] = predicted_list
    output["status_embed"] = embed_list
    output["status_prob"] = predicted_prob_list
    return jsonify(output)


@app.route('/test', methods=['get'])
def test():
    return "test"


# 全局异常处理装饰器
#@app.errorhandler(Exception)
#def handle_exception(e):
    # 返回异常信息
    # 打印错误日志到文件
#    logging.error('Unhandled Exception: %s', str(e))
#    response = {
#        'error': str(e)
#    }
#    return jsonify(response), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=9803,debug=True)
