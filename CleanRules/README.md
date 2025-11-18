# Introduction

## 1. Preliminary Work
1. Corpus file format conversion: Script for converting Parquet to JSON format: parquet_to_json.py
2. Large corpus file splitting: Supports multiprocessing with process pools for efficient cleaning; splitting script: 1-bigfile_split_500M_mindspore_dataset.py
## 2. Cleaning Rules
Concurrent corpus cleaning rules script: rules_clean_multi_process.py
### 2.1 Data Record Cleaning Rules
- Calculate the proportion of punctuation characters and filter out data records with excessively low punctuation ratios.
    - Rule: Remove the entire data record.
    - Code:
```
    # 标点符号过多
    punctuation = '|_>.·*'
    punctuation_string = string.punctuation
    punc_count = sum([content.count(char) for char in punctuation_string])
    if 1.0*(punc_count)/len(content) > 0.5:
        is_delete = True
        return is_delete,content
```
- Consecutive Special Characters
    - Rule：Remove the consecutively matched strings identified by regular expression
    - Code:
```
    # 连续特殊符号，则删除字符
    pattern_symbol_same = re.compile(r"[-]{4,}|[.]{4,}|[=]{4,}", re.S)
    content = re.sub(pattern_symbol_same, ' ', content)
```
- Meaningless Special Characters
    - Rule：Remove meaningless special characters
    - Code:
```
    # 特殊符号且无含义，则只删除字符
    pattern_symbol_special = re.compile(r"[\¤\⌒\々\〓\▌\◇\▲\△\▪\★\◆\▼\●\▽\◁\☆\○]", re.S)
    content = re.sub(pattern_symbol_special, '', content)
```
- HTML Content Extraction
    - Rule：Extract only valid data from HTML-formatted corpus content
    - Code:
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
### 2.2 Line Cleaning Rules
- Empty Lines
    - Rule：Delete the line data
    - Code:
```
    if line == '\n':
        is_delete = True
        return is_delete
```
- Calculate the proportion of special characters in line data, with a lower threshold limit
    - Rule：If the proportion exceeds the threshold, delete the line data
    - Code:
```
    punctuation = '|_>.·*'
    if line:
        punc_count = sum([line.count(char) for char in punctuation])
        rate = 1.0 * punc_count / len(line)
        if rate > 0.5:
            is_delete = True
            return is_delete
```
## 3. Cleaning Examples
- Cleaning of consecutive special characters
    - Example before/after cleaning
```
Before:
cat test.json
{"text": "你好啊，我就是測試下，然後學習下。----------------this..................that===================these--those...---------------"}

After:
cat test_2023-10-16-17-19-26_clean.json
{"text": "你好啊，我就是測試下，然後學習下。 this that these--those... "}
```
## 4. Post-processing
1. Merge the cleaned result files using the merge script: merge.sh
2. Perform sample deduplication on the cleaned output files
### 4.1 Quality Evaluation
1. Use models to evaluate corpus quality
2. Conduct manual sampling review to evaluate corpus quality
