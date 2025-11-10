### 中文领域分类模型
* 训练数据构建：采用GPT3.5模型API生成1W条文本分类数据、C-Eval学科数据形成了分类数据
* 模型选择：基于BERT的中文文本分类模型
* 模型训练：
```
    python run.py --model bert --data_path /DATA_PATH/ 
```
* 模型推理：
```
    python run.py --model bert --data_path /DATA_PATH/ --result_data_path /RESULT_SAVE_PATH/
```




##  鹏城脑海模型中文数据领域分布结果
### 中文文本数据领域分类体系
综合参考《中国图书馆图书分类法》与《中文学科体系分类方法》，以及已有数据情况，形成40个中文文本数据领域，包括：哲学、政治、法律、军事、经济、体育、文学、艺术、地理、历史、教育学、管理学、数学科学、物理科学、化学科学、生物科学、医学、农业科学、轻工业、重工业、交通运输、天文学、环境科学、电子信息科学、计算机科学、通信技术、材料科学、能源科学、食品科学、游戏娱乐、休闲旅游、心理学、城市规划与建设、影视影评、气象学、广告营销、电子商务、宗教、安全科学与消防工程、其他。
### 数据领域分布
* 待整理...

## Reference
[1] BERT: Pre-training of Deep Bidirectional Transformers for Language Understanding  
[2] ERNIE: Enhanced Representation through Knowledge Integration 
[3] DoReMi: Optimizing Data Mixtures Speeds Up Language Model Pretraining
