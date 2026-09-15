# CCNet Filter

Quality assessment and filtering of English corpora based on **KenLM perplexity (PPL)**. The approach follows [CCNet](https://github.com/facebookresearch/cc_net): score documents with a language model, then bucket them by thresholds or keep a top fraction of higher-quality samples.

## Overview

1. Tokenize with **SentencePiece**
2. Compute perplexity (PPL) with **KenLM**
3. Map scores to percentile cutoffs in `cutoff.csv` and assign each sample to:
   - `head`: lower PPL (closer to the LM distribution; usually higher quality)
   - `middle`
   - `tail`: higher PPL (noisier / lower quality)
4. (Optional) Sort by PPL and keep only a given fraction of samples

Default cutoffs: `percentile_head = 30`, `percentile_tail = 60` (from the `en` column of `cutoff.csv`).

## Layout

```
cc/
├── en.sp.model              # English SentencePiece tokenizer
├── en.sp.vocab              # vocabulary
├── cutoff.csv               # per-language PPL percentile cutoffs
├── kenlm_zyh_quality_assessment.py   # local: PPL + bucketing
├── en-kenlm_quality_ppl.py           # cloud (ModelArts/OBS): PPL + bucketing
└── en_1-cat_ppl_merge.py             # cloud: sort by PPL and keep/drop by ratio
```

> You also need the KenLM binary model `en.arpa.bin` (referenced in the scripts). Place it at the expected path.

## Dependencies

- Python 3
- `sentencepiece`
- `kenlm`
- `pandas`
- Cloud scripts additionally require `moxing` (Huawei Cloud OBS) and a multiprocessing environment

## Usage

### 1. Local scoring and bucketing

`kenlm_zyh_quality_assessment.py`

- Input: `./sample_for_filter/*.json` (JSONL; each line has a `text` field)
- Output: `./sample_for_filter_processed/` (original fields plus `perplexity` and `bucket`)
- Expected model paths: `./data/en.sp.model`, `./data/en.arpa.bin`, `./data/cutoff.csv`
- Multithreaded. Processed files are recorded in `processed_files.txt` so runs can resume.

```bash
python kenlm_zyh_quality_assessment.py
```

### 2. Cloud batch scoring (OBS)

`en-kenlm_quality_ppl.py`

Pulls JSON from OBS, computes PPL and bucket, then writes results back to OBS.

Main arguments:

| Argument | Description |
|------|------|
| `--input_obs_dir` | Input OBS directory |
| `--data_name` | Subdirectory / dataset name |
| `--output_obs_dir` | Output OBS directory |
| `--rank` / `--task_num` | Shard index / number of tasks |
| `--num_process` | Number of processes (default: 24) |

### 3. Filter by perplexity

`en_1-cat_ppl_merge.py`

Sorts records that already have `perplexity` in ascending order, keeps the top `ratio`, and writes the rest to discarded.

| Argument | Description |
|------|------|
| `--ratio` | Keep ratio (default: `0.7`) |
| `--input_obs_dir` / `--output_obs_dir` | OBS input / output |
| `--data_name` | Dataset name |
| `--num_process` | Number of processes |

Example outputs:

- `reserved/{data_name}.json`: kept samples
- `discarded/{data_name}-last_0.30.json`: dropped samples (when `ratio=0.7`)

## Input format

JSONL, one object per line, with at least:

```json
{"text": "your document text here"}
```

After processing, extra fields are added:

```json
{"text": "...", "perplexity": 123.4, "bucket": "head"}
```

## Recommended pipeline

```
raw JSONL
    │
    ▼
en-kenlm_quality_ppl.py / kenlm_zyh_quality_assessment.py   # compute PPL and buckets
    │
    ▼
en_1-cat_ppl_merge.py                                       # keep top-k% by PPL
    │
    ▼
high-quality corpus (reserved)
```

## Notes

- This repo focuses on English (`en`). `cutoff.csv` also contains cutoffs for other languages and can be extended.
- The cloud scripts target Huawei Cloud ModelArts + OBS (`moxing`). For local use, switch to the local script and local paths.
