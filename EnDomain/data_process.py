import sys
import os
import json
import time
import re
import pandas as pd
from pandas import DataFrame
from pandas import ExcelWriter
import jieba
from simhash import Simhash
import string
import random

train_f=open("./train.json", "w+")
#label_f=open("./label.json", "w+")
data_dis_f=open("./data_distribute.json", "w+")
data_dis_dict={}


label_z_f=open("./label.json",'w+')


#label_z_dict=json.load(label_z_f)

label_dict={}
#label_dict=label_z_dict
#index=max(list(label_dict.values()))+1

#ceval_t_pt_label_dict={}
#for line in open("ceval_2_ptlabel.txt",'r+').readlines():
#    line=line.strip()
#    line_split=line.split(" ")
#    ceval_t_pt_label_dict[line_split[1]]=line_split[0]
'''
all_class_list = ["哲学", "政治", "法律", "军事", "经济", "体育", "文学", "艺术", "地理", "历史",
                      "教育学", "管理学", "数学科学", "物理科学", "化学科学", "生物科学", "医学",
                      "农业科学", "轻工业", "重工业","交通运输", "天文学", "环境科学", "电子信息科学",
                      "计算机科学", "通信技术","材料科学", "能源科学", "食品科学", "游戏娱乐", "休闲旅游", "心理学",
                      "城市规划与建设", "影视影评", "气象学", "广告营销", "电子商务", "宗教","安全科学与消防工程","其他"]





need_class_list=["生物科学", "农业科学", "天文学", "材料科学", "城市规划与建设", "能源科学", "地理", "通信技术", "气象学", "物理科学", "数学科学", "化学科学"]
#need_class_list=["政治"]

need_xueke=open("detail/need_xueke.json","w+")
need_law=open("detail/need_xueke.json","w+")
need_policy=open("detail/need_policy.json","w+")
need_zhexue=open("detail/need_zhexue.json","w+")
need_junshi=open("detail/need_junshi.json","w+")
need_economy=open("detail/need_economy.json","w+")
need_sport=open("detail/need_sport.json","w+")
need_wenxue=open("detail/need_wenxue.json","w+")
need_art=open("detail/need_art.json","w+")
need_his=open("detail/need_his.json","w+")
need_tech=open("detail/need_tech.json","w+")
need_manage=open("detail/need_manage.json","w+")
need_medical=open("detail/need_medical.json","w+")
need_qinggongye=open("detail/need_qinggongye.json","w+")
need_zhonggongye=open("detail/need_zhonggongye.json","w+")
need_transport=open("detail/need_transport.json","w+")
need_tianwenxue=open("detail/need_tianwenxue.json","w+")
need_env=open("detail/need_env.json","w+")
need_electronic=open("detail/need_electronic.json","w+")
need_computer=open("detail/need_computer.json","w+")
need_env=open("detail/need_env.json","w+")
need_food=open("detail/need_food.json","w+")
need_game=open("detail/need_game.json","w+")
need_travel=open("detail/need_travel.json","w+")
need_psychology=open("detail/need_psychology.json","w+")
need_urban=open("detail/need_urban.json","w+")
need_video=open("detail/need_video.json","w+")
need_adv=open("detail/need_adv.json","w+")
need_ecom=open("detail/need_e-com.json","w+")
need_religion=open("detail/need_religion.json","w+")
need_safe=open("detail/need_safe.json","w+")
need_other=open("detail/need_other.json","w+")

all_class_dict = {"哲学":need_zhexue, "政治":need_policy, "法律":need_law, "军事":need_junshi, "经济":need_economy, "体育":need_sport,
                "文学":need_wenxue, "艺术":need_art, "地理":need_xueke, "历史":need_his, "教育学":need_tech, "管理学":need_manage, "数学科学":need_xueke, 
                "物理科学":need_xueke, "化学科学":need_xueke, "生物科学":need_xueke, "医学":need_medical, "农业科学":need_xueke, "轻工业":need_qinggongye, 
                "重工业":need_zhonggongye, "交通运输":need_transport, "天文学":need_tianwenxue, "环境科学":need_env, "电子信息科学":need_electronic,
                "计算机科学":need_computer, "通信技术":need_xueke,"材料科学":need_xueke, "能源科学":need_xueke, "食品科学":need_food, "游戏娱乐":need_game, 
                "休闲旅游":need_travel, "心理学":need_psychology, "城市规划与建设":need_urban, "影视影评":need_video, "气象学":need_xueke, "广告营销":need_adv, 
                "电子商务":need_ecom, "宗教":need_religion,"安全科学与消防工程":need_safe,"其他":need_other}

'''

def json_process(data_path):
    global index
    global label_dict
    global data_dis_dict
    index=0
    df = open(data_path,"r")
    for line in df.readlines():
        line_d=json.loads(line)
        q_d={}
        need_d={}
        question=str(line_d["text"])
        q_d["question"]=question
        need_d["text"]=question
        need_d["gen_content"]=line_d["gen_content"]
        need_d["predict_domain"]=line_d["predict_domain"]
        label=line_d["predict_domain"]
        if label in label_dict.keys():
            q_d["label"]=label_dict[label]
        else:
            label_dict[label]=index
            index=index+1
            q_d["label"]=label_dict[label]
        if label in data_dis_dict.keys():
            data_dis_dict[label]=data_dis_dict[label]+1
        else:
            data_dis_dict[label]=1
        #print(data_dis_dict)
    
        train_f.write(json.dumps(q_d, ensure_ascii=False)+"\n")
        label_z_f.write(json.dumps(label_dict, ensure_ascii=False)+"\n")
        #need_f=all_class_dict[label]
        #need_f.write(json.dumps(need_d, ensure_ascii=False)+"\n")

    data_dis_f.write(json.dumps(data_dis_dict, ensure_ascii=False)+"\n") 

def process(eval_path):
    
    for parent, dirpath, filelist in os.walk(eval_path):
        for filename in filelist:
            print(parent,filename)
            if filename=="chatgpt_sample.json" and parent.find("history")==-1:
                #model_type = filename[:-4].split("_")[0]
                #try:
                #label_name=("_").join(filename.split("_")[:-1])
                json_process(parent+"/"+filename)
    
    
eval_path=sys.argv[1]
process(eval_path)
#label_f.write(json.dumps(label_dict, ensure_ascii=False)+"\n")
print(data_dis_dict)