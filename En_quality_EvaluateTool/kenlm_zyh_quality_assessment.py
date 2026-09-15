import sentencepiece as spm
import kenlm
import pandas as pd
import math
import time
import json
import os
import concurrent.futures

# 加载分词器
sp = spm.SentencePieceProcessor()
sp.load('./data/en.sp.model')

# 加载语言模型
lm = kenlm.Model('./data/en.arpa.bin')

# 读取 cutoff.csv
cutoff_df = pd.read_csv('./data/cutoff.csv', index_col=0, encoding='utf-8')
en_cutoffs = cutoff_df['en']

# 定义百分位数
percentile_head = 30
percentile_tail = 60

# 获取困惑度阈值
pp_head = en_cutoffs.loc[percentile_head]
pp_tail = en_cutoffs.loc[percentile_tail]

# 记录已处理文件的文件名
PROCESSED_FILES_LOG = 'processed_files.txt'

def load_processed_files():
    """加载已处理的文件列表"""
    if os.path.exists(PROCESSED_FILES_LOG):
        with open(PROCESSED_FILES_LOG, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f)
    return set()

def log_processed_file(file_name):
    """记录已处理的文件"""
    with open(PROCESSED_FILES_LOG, 'a', encoding='utf-8') as f:
        f.write(file_name + '\n')

def compute_perplexity(text, sp, lm):
    tokens = sp.encode(text, out_type=str)
    tokenized_text = ' '.join(tokens)
    logprob_sum = 0.0
    num_tokens = len(tokens)
    for word, logprob, length in lm.full_scores(tokenized_text):
        logprob_sum += logprob
    # 将 logprob_sum 从自然对数转换为以 2 为底的对数
    logprob_sum /= math.log(2)
    perplexity = pow(2, - (logprob_sum / num_tokens))
    return perplexity

def assign_bucket(perplexity, pp_head, pp_tail):
    if perplexity < pp_head:
        return 'head'
    elif perplexity < pp_tail:
        return 'middle'
    else:
        return 'tail'

def process_file(file_path, output_dir):
    try:
        file_name = os.path.basename(file_path)
        output_file = os.path.join(output_dir, file_name)

        # 如果输出文件已存在，则跳过处理
        if os.path.exists(output_file):
            print(f"文件 {file_name} 已处理，跳过")
            return

        # 读取 JSON 文件
        with open(file_path, 'r', encoding='utf-8') as f:
            data = [json.loads(line) for line in f]

        # 处理每一行数据
        for item in data:
            text = item.get('text', '')
            perplexity = compute_perplexity(text, sp, lm)
            bucket = assign_bucket(perplexity, pp_head, pp_tail)
            item['perplexity'] = perplexity
            item['bucket'] = bucket

        # 保存处理后的数据到新文件
        with open(output_file, 'w', encoding='utf-8') as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')

        # 记录已处理的文件
        log_processed_file(file_name)
        print(f"文件 {file_name} 处理完成，结果保存到 {output_file}")

    except Exception as e:
        print(f"处理文件 {file_name} 时出错: {e}")

def main():
    # 输入目录和输出目录
    input_dir = './sample_for_filter/'
    output_dir = './sample_for_filter_processed'

    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    # 获取所有 JSON 文件
    json_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.json')]
    # print("json_files", json_files)
    # 加载已处理的文件列表
    processed_files = load_processed_files()

    # 过滤掉已处理的文件
    json_files = [f for f in json_files if os.path.basename(f) not in processed_files]
    # print("json_files", json_files)

    # 如果没有需要处理的文件，直接退出
    if not json_files:
        print("所有文件已处理完成，无需重新处理。")
        return

    # 记录总运行时间
    total_start_time = time.time()

    # 使用 ThreadPoolExecutor 并行处理文件
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # 提交任务到线程池
        futures = [executor.submit(process_file, file_path, output_dir) for file_path in json_files]

        # 等待所有任务完成
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # 获取任务结果，如果有异常会在这里抛出
            except Exception as e:
                print(f"处理文件时出错: {e}")

    # 计算总运行时间
    total_end_time = time.time()
    total_elapsed_time = total_end_time - total_start_time

    print(f"所有文件处理完成，总运行时间: {total_elapsed_time:.3f} 秒")

if __name__ == '__main__':
    main()