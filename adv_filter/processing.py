import re
import json
import os
from hanziconv import HanziConv

word_dictionary_path = 'data/word_dictionary.txt'
rawdata_path = 'data/rawdata/text.txt'
path_po = os.path.join(os.getcwd(), r'data/dealdata/positive.txt')
path_ne = os.path.join(os.getcwd(), r'data/dealdata/negative.txt')

if not os.path.exists(path_po):
	with open(path_po, 'w') as f_po:
		print("positive.txt 文件创建成功")

if not os.path.exists(path_ne):
	with open(path_ne, 'w') as f_ne:
		print("negative.txt 文件创建成功")

def tradition_to_simple(input_text):
	output_text = HanziConv.toSimplified(input_text)
	return output_text

ad_list = []
path_ad = os.path.join(os.getcwd(), word_dictionary_path)
with open(path_ad, 'r', encoding='utf-8') as f:
	for item in f.readlines():
		if len(item.strip()) > 0:
			ad_list.append(tradition_to_simple(item.strip()))


def into_positive(content_list):
	with open(path_po, 'w', encoding='utf-8') as f:
		for item in content_list:
			if len(item) > 0:
				f.write(item + '###' + '1')
				f.write('\n')


def into_negative(content_list):	
	with open(path_ne, 'w', encoding='utf-8') as f:
		for item in content_list:
			if len(item) > 0:
				f.write(item + '###' + '0')
				f.write('\n')

def judgetext(input_text, advertise_list):
	flag = 1
	for item in advertise_list:
		if item in input_text or tradition_to_simple(item) in input_text:
			flag = 0
	return flag

with open(rawdata_path, 'r', encoding = 'utf-8') as f:
	content = f.read()

pattern = r"\{[^{}]*\}"
sentences_list = re.findall(pattern, content)

into_positive_list = []
into_negative_list = []

for idx, item in enumerate(sentences_list):
	print('Dealing', idx,  'text')
	flag = 1
	temp_list = []
	try:
		json_item = json.loads(item)
		temp_list =	json_item['text'].split('\n')
		for elem in temp_list:
			flag = judgetext(elem, ad_list)
			if flag > 0:
				into_positive_list.append(elem)
				# into_positive(elem)
			else:
				into_negative_list.append(elem)
				# into_negative(elem)
	except:
		pass


into_positive(into_positive_list)
into_negative(into_negative_list)
