import json


def read_hg_data(input_file_path, output_file_path):
    print("------------------------")
    print("文件名：{}",input_file_path)
    data_filter = []
    seen = set()
    total_lines = 0
    skip_lines = 0
    
    with open(input_file_path, 'r') as file, open(output_file_path, 'w') as output:
        for i, content in enumerate(file):
            text_json = json.loads(content)
            for line in text_json:
                if 'chat' in line['input']:
                    label = '###0' if line['output'] == 'T' else '###1' 
                    test_list = line['input']['chat'].replace('\\n','').replace('\n','')+ label+'\n'
                    output.write(test_list)

    return data_filter

#^.{0,12}\n

read_hg_data('/home/zlj/code/pcl/template/data/datahug/tg_ad_qa.json', '/home/zlj/code/pcl/template/data/1text/tg_ad_qa.txt')