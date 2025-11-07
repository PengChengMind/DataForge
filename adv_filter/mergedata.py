import os

path_raw = os.path.join(os.getcwd(), r'data/rawdata/text')
path_w = os.path.join(os.getcwd(), r'data/rawdata/text.txt')
dir_list = os.listdir(path_raw)

if not os.path.exists(path_raw):
    os.makedirs(path_raw)

def read_content(path):
	content_list = []
	with open(path, 'r', encoding='utf-8') as f:
		for elem in f.readlines():
			if len(elem) > 0:
				content_list.append(elem.strip())
	return content_list

content_sum_list = []
for path_elem in dir_list:
	path_temp = os.path.join(path_raw, path_elem)
	content_temp_list = read_content(path_temp)
	content_sum_list = content_sum_list + content_temp_list


with open(path_w, 'w', encoding='utf-8') as fw:
	for elem in content_sum_list:
		fw.write(elem.strip())
		fw.write('\n')
