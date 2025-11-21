# coding: UTF-8
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from sklearn import metrics
import time
from utils import get_time_dif
from importlib import import_module
import argparse
from utils import build_dataset, build_iterator, get_time_dif
from utils_mul import build_dataset, build_iterator, get_time_dif,build_infer_dataset,build_infer_dataset_multiprocess
import json
import os
import glob

# parser = argparse.ArgumentParser(description='Chinese Text Classification')
# parser.add_argument('--model', type=str, required=True, help='choose a model: Bert, ERNIE')
# parser.add_argument('--data_path', type=str, required=True, help='./')

parser = argparse.ArgumentParser(description='Chinese Text Classification')
parser.add_argument('--model', type=str, required=True, help='choose a model: Bert, ERNIE')
parser.add_argument('--data_path', type=str, required=True, help='./')
parser.add_argument('--result_data_path', type=str, required=True, help='./tmp/')
parser.add_argument('--batch_size', type=int, required=True, default=16, help='batch_size')
parser.add_argument('--prefix', type=str, required=True, default=16, help='zh_cc')
parser.add_argument('--device_id', type=str, required=True, default=16, help='zh_cc')

args = parser.parse_args()
print('args.device_id:',str(args.device_id))
os.environ['CUDA_VISIBLE_DEVICES'] = str(args.device_id)


all_class_list = ["哲学", "政治", "法律", "军事", "经济", "体育", "文学", "艺术", "地理", "历史",
                      "教育学", "管理学", "数学科学", "物理科学", "化学科学", "生物科学", "医学",
                      "农业科学", "轻工业", "重工业","交通运输", "天文学", "环境科学", "电子信息科学",
                      "计算机科学", "通信技术","材料科学", "能源科学", "食品科学", "游戏娱乐", "休闲旅游", "心理学",
                      "城市规划与建设", "影视影评", "气象学", "广告营销", "电子商务", "宗教","安全科学与消防工程","其他"]
all_class_list=[]

label_f=open("data/1_sample/label.json",'r')
label_dict=json.load(label_f)
label_id_dict={}
for k in label_dict.keys():
    all_class_list.append(k)
    label_id_dict[label_dict[k]]=k

# 权重初始化，默认xavier
def init_network(model, method='xavier', exclude='embedding', seed=123):
    for name, w in model.named_parameters():
        if exclude not in name:
            if len(w.size()) < 2:
                continue
            if 'weight' in name:
                if method == 'xavier':
                    nn.init.xavier_normal_(w)
                elif method == 'kaiming':
                    nn.init.kaiming_normal_(w)
                else:
                    nn.init.normal_(w)
            elif 'bias' in name:
                nn.init.constant_(w, 0)
            else:
                pass

def statics(t,p,error):
    f=open('error.txt', 'w+')
    classes_correct_dict={}
    classes_pre_dict={}
    classes_t_dict={}
    for i_t, i_p in zip(t,p):
        if i_t==i_p:
            try:
                classes_correct_dict[i_t]=classes_correct_dict[i_t]+1
            except:
                classes_correct_dict[i_t]=1
        try:
            classes_pre_dict[i_p]=classes_pre_dict[i_p]+1
        except:
            classes_pre_dict[i_p]=1
        try:
            classes_pre_dict[i_t]=classes_pre_dict[i_p]+1
        except:
            classes_pre_dict[i_t]=1
    for l in classes_t_dict.keys():
        if l in classes_correct_dict.keys():
            print(classes_correct_dict[l]/classes_pre_dict[l], classes_correct_dict[l]/classes_t_dict[l])
    for e in error:
        f.write(e+"\n")



PAD, CLS = '[PAD]', '[CLS]'  # padding符号, bert中综合信息符号
SEP = '[SEP]'  # padding符号, bert中综合信息符号

# def predict_res(config, model, data_path):
#     # test
#     model.load_state_dict(torch.load(config.save_path))
#     model.eval()
#     start_time = time.time()
#     with torch.no_grad():
#         for parent, dirpath, filelist in os.walk(data_path):
#             print('parent:', parent)
#             for filename in filelist:
#                 w_f=open("/code/en_domain_dataset_labeled/"+filename,"w+")
#                 with open(parent+"/"+filename, 'r', errors='ignore') as f:
#                     #data_info = json.load(f)
#                     index=0
#                     predict_all = np.array([], dtype=int)
#                     for line in f.readlines():
#                         try:
#                             index+=1
#                             #if index>10:
#                             #    break
#                             line=line.strip()
#                             ori=line
#                             line=eval(line)
#                             token_1 = config.tokenizer.tokenize(line["text"])
#                             #token_2 = config.tokenizer.tokenize(line["output"])
#                             token = [CLS] + token_1# + [SEP] +token_2
#                             seq_len = len(token)
#                             mask = []
#                             token_ids = config.tokenizer.convert_tokens_to_ids(token)
#
#                             if config.pad_size:
#                                 if len(token) < config.pad_size:
#                                     mask = [1] * len(token_ids) + [0] * (config.pad_size - len(token))
#                                     token_ids += ([0] * (config.pad_size - len(token)))
#                                 else:
#                                     mask = [1] * config.pad_size
#                                     token_ids = token_ids[:config.pad_size]
#                                     seq_len = config.pad_size
#                             test=(torch.tensor([token_ids]).to(config.device),torch.tensor([seq_len]).to(config.device),torch.tensor([mask]).to(config.device))
#                             #print(test)
#                             outputs = model(test)
#                             predic = torch.max(outputs.data, 1)[1].cpu().numpy()
#                             predict_all = np.append(predict_all, predic)
#                             temp={}
#                             temp["text"]=line["text"]
#                             temp["label"]=label_id_dict[predic[0]]
#                             w_f.write(json.dumps(temp, ensure_ascii=False)+"\n")
#                         except:
#                             continue

    


def predict_batch(config, model,data_path, infer_res_save_path, start_prefix):
    # test
    model.load_state_dict(torch.load(config.save_path))
    model.eval()

    data_files = glob.glob("/" + start_prefix)
    print(f"data_files:{data_files}")
    data_file_iter = iter(data_files)
    start_time = time.time()
    data_num = 0
    data_load_time = 0
    for file_name in data_file_iter:
        if os.path.exists(infer_res_save_path + file_name.split("/")[-1]):
            print("other process predict ing")
            continue
        t1 = time.time()
        # print(f"file_name:", file_namme)
        # print(f"save_file-name:", file_namme.split("/")[-1])
        save_file = open(infer_res_save_path + file_name.split("/")[-1], 'w', encoding='UTF8')
        test_dataset_iter = build_infer_dataset_multiprocess(config, file_name)
        current_file_load_time = time.time() - t1
        data_load_time += current_file_load_time
        print("data loaded")
        with torch.no_grad():
            try:
                for texts_tokens, text_contents_dicts in test_dataset_iter:
                        data_num += config.batch_size
                        outputs = model(texts_tokens)
                        score = torch.max(outputs.data, 1)[0].cpu().numpy()
                        predict = torch.max(outputs.data, 1)[1].cpu().numpy()
                        for indx in range(len(text_contents_dicts)):
                            text_contents_dicts[indx]["label"] = label_id_dict[predict[indx]]
                            text_contents_dicts[indx]["label_score"] = str(score[indx])
                            save_file.write(json.dumps(text_contents_dicts[indx], ensure_ascii=False) + '\n')
                        spend_time = time.time() - start_time
                        if data_num % (config.batch_size * 50) == 0:
                            print(
                                f"---infer speed:{spend_time / data_num} s/sample------data num:{data_num}----curren file load:{current_file_load_time}------")

            except Exception as e:
                print(str(e))
                pass
    print("infer finished")
    spend_time = time.time() - start_time
    print(f"---infer speed:{spend_time / data_num} s/sample------data num:{data_num}---")
    print(f"-----data_load_time:{data_load_time}----total time:{spend_time}----")


if __name__ == '__main__':
    dataset = 'data/4_sample/'  # 数据集

    model_name = args.model  # bert
    data_path=args.data_path
    start_prefix = args.prefix
    result_data_path = args.result_data_path
    x = import_module('models.' + model_name)
    config = x.Config(dataset)
    np.random.seed(1)
    torch.manual_seed(1)
    torch.cuda.manual_seed_all(1)
    torch.backends.cudnn.deterministic = True  # 保证每次结果一样
    start_time = time.time()
    # train
    model = x.Model(config).to(config.device)
    config.batch_size = args.batch_size
    predict_batch(config, model, data_path,result_data_path, start_prefix)#, train_iter, dev_iter, test_iter)
