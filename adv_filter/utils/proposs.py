# 确保文件路径正确
input_file_path = '/home/zlj/code/pcl/template/data/dataset/porn/positive.txt'
output_file_path = '/home/zlj/code/pcl/template/data/dealdata/positive.txt'

# 使用集合来存储已经出现过的行
seen = set()
with open(input_file_path, 'r') as file, open(output_file_path, 'w') as output:
    for line in file:
        if line not in seen:
            seen.add(line)
            output.write(line)