# 数据清洗说明

## 1. 前期工作
1. 语料文件格式转换，parquet转json格式脚本：parquet_to_json.py
2. 语料大文件切分，支持清洗功能的进程池多进程并发，切分脚本：1-bigfile_split_500M_mindspore_dataset.py
## 2. 清洗规则
并发语料规则清洗脚本：rules_clean_multi_process.py
### 2.1 数据记录清洗规则
- 统计标点符号比例，过滤掉标点符号比例过低的数据
    - 处理逻辑：删除整条数据记录
    - 代码逻辑
```
    # 标点符号过多
    punctuation = '|_>.·*'
    punctuation_string = string.punctuation
    punc_count = sum([content.count(char) for char in punctuation_string])
    if 1.0*(punc_count)/len(content) > 0.5:
        is_delete = True
        return is_delete,content
```
- 连续特殊字符
    - 处理逻辑：删除正则匹配到的连续字符串
    - 代码逻辑
```
    # 连续特殊符号，则删除字符
    pattern_symbol_same = re.compile(r"[-]{4,}|[.]{4,}|[=]{4,}", re.S)
    content = re.sub(pattern_symbol_same, ' ', content)
```
- 无语义特殊字符
    - 处理逻辑：删除无语义特殊字符
    - 代码逻辑
```
    # 特殊符号且无含义，则只删除字符
    pattern_symbol_special = re.compile(r"[\¤\⌒\々\〓\▌\◇\▲\△\▪\★\◆\▼\●\▽\◁\☆\○]", re.S)
    content = re.sub(pattern_symbol_special, '', content)
```
- html内容提取
    - 处理逻辑：语料数据html格式内容，只做有效数据提取
    - 代码逻辑
```
def html2plaintext(html):
    soup = BeautifulSoup(html, features="html.parser")

    try:
        if soup.div["id"]=="copyright":
            return ""
    except:
        a=0
    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()  # rip it out
    divs = soup.find_all('div')
    for div in divs:
        for a in div.find_all('a'):
            a.replace_with_children()
    # get text
    text = soup.get_text()
    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)
    # print(text)
    return text
```
### 2.2 数据行清洗规则
- 空行
    - 处理逻辑：删除行数据
    - 代码逻辑
```
    if line == '\n':
        is_delete = True
        return is_delete
```
- 统计行数据特殊字符比例，限制低于某一阈值
    - 处理逻辑：若大于阈值，则删除行数据
    - 代码逻辑
```
    punctuation = '|_>.·*'
    if line:
        punc_count = sum([line.count(char) for char in punctuation])
        rate = 1.0 * punc_count / len(line)
        if rate > 0.5:
            is_delete = True
            return is_delete
```
## 3. 清洗示例
- 针对连续特殊字符的清洗
    - 清洗前/后示例
```
清洗前
cat test.json
{"text": "你好啊，我就是測試下，然後學習下。----------------this..................that===================these--those...---------------"}

清洗后
cat test_2023-10-16-17-19-26_clean.json
{"text": "你好啊，我就是測試下，然後學習下。 this that these--those... "}
```
## 4. 后期工作
1. 清理结果文件进行合并操作，合并的命令脚本：merge.sh
2. 清洗后结果文件，进行样本去重
### 4.1 质量评估
1. 使用模型进行语料质量评估
2. 人工抽审进行语料质量评估
