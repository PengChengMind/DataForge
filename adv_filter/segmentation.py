import os

train_content_list = []
test_content_list = []


count_negative = 0
path = os.path.join(os.getcwd(), r'data/dealdata/negative.txt')
with open(path, 'r', encoding='utf-8') as f:
	f_list = f.readlines()
	test_num_nega = int(len(f_list) * 0.1)
	for item in f_list:
		if count_negative < test_num_nega:
			test_content_list.append(item.strip())
		else:
			train_content_list.append(item.strip())
		count_negative += 1
f.close()

count_positive = 0
path1 = os.path.join(os.getcwd(), r'data/dealdata/positive.txt')
with open(path1, 'r', encoding='utf-8') as f1:
	f1_list = f1.readlines()
	test_num_posi = int(len(f1_list) * 0.1)
	for item in f1_list:
		if count_positive < test_num_posi:
			test_content_list.append(item.strip())
		else:
			train_content_list.append(item.strip())
		count_positive += 1
f1.close()


print("测试样本数为：", len(test_content_list))
print("训练样本数为：", len(train_content_list))


path_train = os.path.join(os.getcwd(), r'data/dataset/traindata.txt')
with open(path_train, 'w', encoding='utf-8') as ftrain:
	for item in train_content_list:
		ftrain.write(item)
		ftrain.write('\n')
ftrain.close()

path_test = os.path.join(os.getcwd(), r'data/dataset/testdata.txt')
with open(path_test, 'w', encoding='utf-8') as ftest:
	for item in test_content_list:
		ftest.write(item)
		ftest.write('\n')
ftrain.close()
