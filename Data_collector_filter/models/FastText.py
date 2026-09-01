# coding: UTF-8
import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np


class Config(object):

    """配置参数"""
    def __init__(self, dataset, embedding):
        self.model_name = 'FastText'
        self.train_path = dataset + '/data/fastdata_train.txt'                                # 训练集
        self.dev_path = dataset + '/data/fastdata_eval.txt'                                    # 验证集
        self.test_path = dataset + '/data/fastdata_eval.txt'                                  # 测试集
        self.predict_path = None  # 预测
        self.class_list = [x.strip() for x in open(
            dataset + '/data/class.txt', encoding='utf-8').readlines()]              # 类别名单
        self.vocab_path = dataset + '/data/vocab.pkl'                                # 词表
        self.save_path = dataset + '/saved_dict/' + self.model_name + '.ckpt'        # 模型训练结果
        self.log_path = dataset + '/log/' + self.model_name
        self.embedding_pretrained = torch.tensor(
            np.load(dataset + '/data/' + embedding)["embeddings"].astype('float32'))\
            if embedding != 'random' else None                                       # 预训练词向量
        self.device = torch.device("cuda" if torch.cuda.is_available() else 'cpu')   # 设备
        #self.device = torch.device('cpu')   # 设备
        self.weight_decay = 1e-5                                        # L2正则系数
        self.dropout = 0.5                                              # 随机失活
        self.require_improvement = 1000                                 # 若超过1000batch效果还没提升，则提前结束训练
        self.num_classes = len(self.class_list)                         # 类别数
        self.n_vocab = 0                                                # 词表大小，在运行时赋值
        self.num_epochs = 20                                            # epoch数
        self.batch_size = 128*10                                          # mini-batch大小
        self.pad_size = 200                                              # 每句话处理成的长度(短填长切)
        self.learning_rate = 1e-3                                       # 学习率
        self.embed = self.embedding_pretrained.size(1)\
            if self.embedding_pretrained is not None else 300           # 字向量维度
        self.hidden_size = 128                                          # 隐藏层大小
        self.n_gram_vocab = 250499                                      # ngram 词表大小
        self.use_ngram = True  # 使用ngram
    def print_config(self):
        print(f"save_path={self.save_path}")
        print(f"device={self.device}")
        print(f"weight_decay={self.weight_decay}")
        print(f"dropout={self.dropout}")
        print(f"require_improvement={self.require_improvement}")
        print(f"num_classes={self.num_classes}")
        print(f"n_vocab={self.n_vocab}")
        print(f"num_epochs={self.num_epochs}")
        print(f"batch_size={self.batch_size}")
        print(f"pad_size={self.pad_size}")
        print(f"learning_rate={self.learning_rate}")
        print(f"hidden_size={self.hidden_size}")
        print(f"n_gram_vocab={self.n_gram_vocab}")
        print(f"use_ngram={self.use_ngram}")
'''Bag of Tricks for Efficient Text Classification'''


class Model(nn.Module):
    def __init__(self, config):
        super(Model, self).__init__()

        self.use_ngram = config.use_ngram

        if config.embedding_pretrained is not None:
            self.embedding = nn.Embedding.from_pretrained(config.embedding_pretrained, freeze=False)
        else:
            self.embedding = nn.Embedding(config.n_vocab, config.embed, padding_idx=config.n_vocab - 1)
        if config.use_ngram:
            self.embedding_ngram2 = nn.Embedding(config.n_gram_vocab, config.embed)
            self.embedding_ngram3 = nn.Embedding(config.n_gram_vocab, config.embed)
            self.fc1 = nn.Linear(config.embed * 3, config.hidden_size)
        else:
            self.fc1 = nn.Linear(config.embed, config.hidden_size)

        self.dropout = nn.Dropout(config.dropout)
        # self.dropout2 = nn.Dropout(config.dropout)
        self.fc2 = nn.Linear(config.hidden_size, config.num_classes)

    def forward(self, x):

        out_word = self.embedding(x[0])
        if self.use_ngram:
            out_bigram = self.embedding_ngram2(x[2])
            out_trigram = self.embedding_ngram3(x[3])
            out = torch.cat((out_word, out_bigram, out_trigram), -1)
        else:
            out = out_word

        out = out.mean(dim=1)
        out = self.dropout(out)
        out = self.fc1(out)
        out = F.relu(out)
        out = self.fc2(out)
        return out
