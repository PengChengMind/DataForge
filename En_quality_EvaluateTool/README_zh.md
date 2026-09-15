 # CCNet Filter

基于 **KenLM 困惑度（Perplexity）** 对英文语料做质量评估与筛选，思路对齐 [CCNet](https://github.com/facebookresearch/cc_net)：用语言模型给文本打分，再按阈值分桶或按比例保留优质样本。

## 原理简述

1. 用 **SentencePiece** 分词
2. 用 **KenLM** 计算文本困惑度（PPL）
3. 对照 `cutoff.csv` 中的百分位阈值，将样本分为：
   - `head`：PPL 较低（更“像”语言模型分布，通常质量较好）
   - `middle`
   - `tail`：PPL 较高（更嘈杂 / 质量较差）
4. （可选）按 PPL 排序，只保留前一定比例的样本

默认阈值：`percentile_head = 30`，`percentile_tail = 60`（取自 `cutoff.csv` 的 `en` 列）。

## 目录结构

```
cc/
├── en.sp.model              # SentencePiece 英文分词模型
├── en.sp.vocab              # 词表
├── cutoff.csv               # 多语言 PPL 百分位阈值表
├── kenlm_zyh_quality_assessment.py   # 本地：计算 PPL + 分桶
├── en-kenlm_quality_ppl.py           # 云端（ModelArts/OBS）：计算 PPL + 分桶
└── en_1-cat_ppl_merge.py             # 云端：按 PPL 排序并按比例保留/丢弃
```

> 另需准备 KenLM 二进制模型 `en.arpa.bin`（脚本中引用，需自行放到对应路径）。

## 依赖

- Python 3
- `sentencepiece`
- `kenlm`
- `pandas`
- 云端脚本额外需要：`moxing`（华为云 OBS）、多进程环境

## 使用说明

### 1. 本地打分与分桶

`kenlm_zyh_quality_assessment.py`

- 输入：`./sample_for_filter/*.json`（JSONL，每行含 `text` 字段）
- 输出：`./sample_for_filter_processed/`（原字段 + `perplexity`、`bucket`）
- 模型路径约定：`./data/en.sp.model`、`./data/en.arpa.bin`、`./data/cutoff.csv`
- 支持多线程；已处理文件会记入 `processed_files.txt`，可断点续跑

```bash
python kenlm_zyh_quality_assessment.py
```

### 2. 云端批量打分（OBS）

`en-kenlm_quality_ppl.py`

从 OBS 拉取 JSON，计算 PPL 与 bucket，再写回 OBS。

主要参数：

| 参数 | 说明 |
|------|------|
| `--input_obs_dir` | 输入 OBS 目录 |
| `--data_name` | 子目录/数据集名 |
| `--output_obs_dir` | 输出 OBS 目录 |
| `--rank` / `--task_num` | 分片任务编号 |
| `--num_process` | 进程数（默认 24） |

### 3. 按困惑度筛选保留

`en_1-cat_ppl_merge.py`

对已带 `perplexity` 的数据按升序排序，保留前 `ratio` 比例，其余写入 discarded。

| 参数 | 说明 |
|------|------|
| `--ratio` | 保留比例（默认 `0.7`） |
| `--input_obs_dir` / `--output_obs_dir` | OBS 输入输出 |
| `--data_name` | 数据集名 |
| `--num_process` | 进程数 |

输出示例：

- `reserved/{data_name}.json`：保留部分
- `discarded/{data_name}-last_0.30.json`：丢弃部分（当 `ratio=0.7`）

## 输入数据格式

JSONL，每行一个对象，至少包含：

```json
{"text": "your document text here"}
```

处理后额外字段：

```json
{"text": "...", "perplexity": 123.4, "bucket": "head"}
```

## 推荐流程

```
原始 JSONL
    │
    ▼
en-kenlm_quality_ppl.py / kenlm_zyh_quality_assessment.py   # 算 PPL、分桶
    │
    ▼
en_1-cat_ppl_merge.py                                       # 按 PPL 保留 top-k%
    │
    ▼
高质量语料（reserved）
```

## 说明

- 本仓库侧重英文（`en`）；`cutoff.csv` 中含多语言阈值，可扩展到其他语种。
- 云端脚本面向华为云 ModelArts + OBS（`moxing`），本地试用请改路径并用本地版脚本。
