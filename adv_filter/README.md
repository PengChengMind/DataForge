### Environments: 
python 3.8, torchtext 0.13.1, torch 1.12.1

### 1 将正负样本negative.txt和positive.txt的样本添加到 data/dealdata

### 2 执行以下命令分割negative.txt和positive.txt样本得到训练集和测试集, 存储在dataset文件夹中
python segmentation.py

### 3 训练模型
python trainmodel.py

