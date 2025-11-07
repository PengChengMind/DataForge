import  json
target1,target2,target3,target4,target5=[],[],[],[],[]
index=0
train={}
test={}
train["train_data"]=[]
test["train_data"]=[]
with open("gen_data/train_score.json", "r", errors='ignore') as train_f:
     prompt_label="基于下面的问题的回复进行打分，分数范围0-7\n问题：{question}\n回答：{anwser}\n回复：{label}"
     data_info = json.load(train_f)
     #print(data_info["train_data"])
     for k in data_info["train_data"]:
        input_str=k["input"]
        output_str=k["output"]
        score=int(k["score"])
        print("问题："+input_str+" "+"回复："+output_str+"\t"+str(score))
        '''
        index+=1
        if index >10000:
            test["train_data"].append(k)
        else:
            train["train_data"].append(k)
        print(k)
        target1.append(k["label_1"])
        target2.append(k["label_2"])
        target3.append(k["label_3"])
        target4.append(k["label_4"])
        target5.append(k["label_5"])
        '''
    
#with open("gen_data/train.json",'w+', encoding='utf-8') as train_f:
#    json.dump(train, train_f, ensure_ascii=False)
#with open("gen_data/test.json", 'w+',encoding='utf-8') as test_f:
#    json.dump(test, test_f, ensure_ascii=False)

#print(len(target1))
#print(sum(target1), sum(target2), sum(target3), sum(target4), sum(target5))
