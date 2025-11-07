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
import csv

train_f=open("./train.json", "w+")
#label_f=open("./label.json", "r")
data_dis_f=open("./data_distribute.json", "w+")
data_dis_dict={}



label_z_f=open("label.json",'r+')

label_z_dict=json.load(label_z_f)


label_dict={}
index=0

ceval_t_pt_label_dict={}
for line in open("label_mmlu",'r+').readlines():
    line=line.strip()
    line_split=line.split("    ")
    if len(line_split)<3:
        continue
    ceval_t_pt_label_dict[line_split[0]]=line_split[2]


def csv_process(data_path, label_name):
    global index
    global label_dict
    global data_dis_dict
    df = pd.read_csv(data_path)
    with open(data_path, encoding='utf-8-sig') as f:
        for df in csv.reader(f, skipinitialspace=True):
            q_d={}
            question=str(df[0])#+" "+str(df[1])+" "+str(df[2])+" "+str(df[3])+" "+str(df[4])
            q_d["question"]=question
            label=ceval_t_pt_label_dict[label_name]
            q_d["label"]=label_z_dict[label.strip()]
            if label in data_dis_dict.keys():
                data_dis_dict[label]=data_dis_dict[label]+1
            else:
                data_dis_dict[label]=1
    
            train_f.write(json.dumps(q_d, ensure_ascii=False)+"\n")
    data_dis_f.write(json.dumps(data_dis_dict, ensure_ascii=False)+"\n") 
    

def process(eval_path):
    
    for parent, dirpath, filelist in os.walk(eval_path):
        for filename in filelist:
            #print(filename)
            if filename[-4:]==".csv":
                #model_type = filename[:-4].split("_")[0]
                #try:
                label_name=("_").join(filename.split("_")[:-1])
                csv_process(parent+"/"+filename, label_name)
    
    
eval_path=sys.argv[1]
process(eval_path)
#label_f.write(json.dumps(label_dict, ensure_ascii=False)+"\n")
