### 英文领域分类模型
* 训练数据构建：通过调用gpt3以及Qwen的模型得到的数据。
  训练语料：2万5千条训练数据
  验证集大小：5000条

* 模型选择：基于BERT的中文文本分类模型
* 模型训练：
```
    python run.py --model bert --data_path /DATA_PATH/ 
```
* 模型推理：
```
    python run.py --model bert --data_path /DATA_PATH/ --result_data_path /RESULT_SAVE_PATH/
```




##  鹏城脑海模型英文数据领域分布结果
### 英文文本数据领域分类体系
综合参考了《美国国会图书馆分类法》与《英文学科体系分类方法》，并结合现有数据情况，形成38个英文文本数据领域，包括："Physics", "Computer Science", "Mathematics", "Materials Science", "Astronomy", "Earth Science", "Engineering and technology", "Others", "Language and literature", "Law","Medicine and health", "Environment studies and forestry", "Communication technology", "Transportation", "Biology", "Education", "Economics","Entertainment and tourism", "Religious Studies", "Arts", "Philosophy", "Business", "Geography","Agriculture", "Political Science","History", "Psychology", "Sociology", "Architecture and design", "Human physical performance and recreation", "Military science","Food science", "Information and communication sciences",
 "Management Science", "Archaeology", "Energy science", "Chemistry", "Aerography".
### 数据领域分布
请查看
[英文领域分布语料](https://openi.pcl.ac.cn/PengChengMind-Corpora/PCMind_en_dataset)

