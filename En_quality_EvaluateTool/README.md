PengCheng-Mind 鹏城·脑海大模型是基于Transformer架构的自回归式语言模型。模型全流程基于中国算力网的全自主安全可控国产软硬件平台进行开发和训练，
采用MindSpore框架实现在大规模集群上长期稳定的多维分布式并行训练。鹏城·脑海模型主要聚焦中文核心能力，兼顾英文和部分多语言能力。 
当前模型已完成训练1T Tokens数据量，仍在持续训练迭代中。

随着中国算力网的不断发展壮大，我们希望通过模型训练全流程开源开放的方式，将鹏城·脑海200B 模型培育为算力网上一个种子应用，
吸引产学研界更多人参与到通用大语言模型基座的研究和应用中，一起助力国产智能软硬件生态的发展。

<img src="./docs/PengCheng-Mind-logo.png" width="600" height="260"/><br/>

## 模型信息

| 模型        |   开源日期   |  预训练token数 |   可下载
|:----------|:--------:|:-------:|:-------:|
| PengChengMind-7B-latest   | 24.11 |                2T    |      ✅ |
| [PengChengMind-7B](https://openi.pcl.ac.cn/PengChengMind/PengChengMind-7B/modelmanage/show_model)  | 23.11 |              1T    |    ✅     |

PengChengMind-7B-latest 更新：
- **batch size调整策略：Loss收敛最优的batchsize方案**
- **学习率调整策略：WSD 、Cosine scheduler**
- **MuP：最优超参不随着width增大而变化**
- **超参数优化：采用了贪心算法依次找到最优配置**
- **数据配比：多组对比实验优化数据配比**


## 要点

- **两千亿级参数量大语言模型「鹏城·脑海」**
- **代码、200B/7B 模型逐步全开源**
- **MindSpore超大规模自动并行技术**
- **模型基于国产全栈式软硬件协同生态（算力网+MindSpore+CANN+昇腾910+ModelArts）**

### 模型结构
|   PengCheng-Mind 7B     |    结构参数      |
| :-------------| :--------------- |
| seq_length    | 4096 | 
| vocab_size    | 125952 | 
| embedding_size    | 4096 | 
| num_layers    | 32 | 
| num_heads    | 32 |

### 环境要求

支持 python >= 3.7.5, mindspore >= 2.1 版本.

推荐使用mindspore的官方 docker 镜像 (http://mirrors.cn-central-221.ovaijisuan.com/mirrors.html)

| 硬件平台       | 操作系统         |框架         |7B推理设备数量  |7B微调设备数量  |
| :------------|:--------------- |:---------- |:------------------|:---------------|
| Ascend 910    | EulerOS-aarch64 |MindSpore   |1卡               |≥16卡             |  

### 模型演化和开源
[鹏城·脑海模型申请](https://openi.pcl.ac.cn/extension/mind)

### 推理

#### 7B 鹏城·脑海模型推理
启动命令（--offline 1代表使用NPU裸机，修改脚本中local_ckpt_path为脑海7B预训练模型文件路径; --offline 0代表使用modelarts环境,修改脚本中gpu_model_npy_obspath为脑海7B预训练模型文件路径;）:
```bash
python predict_new7B.py \
--run_type predict \
--mode 7B \
--vocab_size 125952 \
--seq_length 4096 \
--distribute true \
--use_pynative_op 1 \
--op_level_model_parallel_num 1 \
--device_num 1 \
--stage_num 1 \
--top_p 1.0 \
--top_k_num 3 \
--max_generate_length 100 \
--pre_trained true \
--use_rope True \
--use_past True \
--offline 1
```

### 微调

#### 1、准备训练数据

参考[数据](#数据)部分

#### 2、预训练模型加载，增量训练

启动命令（修改脚本中 obs_ckpt_url为脑海7B预训练模型文件路径。stage开启时，可修改micro_size来调整batch size大小）:

```bash
python train_7B_new2024.py \
--data_url YOUR_OUTPUT_PATH \
--mode 7B \
--vocab_size 125952 \
--seq_length 4096 \
--stage_num 2 \
--op_level_model_parallel 8 \
--micro_size 16 \
--optimizer_shard 1 \
--param_init_type fp16 \
--save_checkpoint True \
--save_checkpoint_steps 100 \
--recompute True \
--sequence_parallel True \
--save_checkpoint_bucket_dir YOUR_SFT_OBS_PATH \
--save_strategy_bucket_dir YOUR_SFT_strategySaving_OBS_PATH \
--use_rope True \
--pre_trained True
```

#### 3、模型合并

finetune 完后的模型是分片的，如果要进行推理，则先需要对模型进行合并。

合并脚本参考：

```bash
python tools/merge_ckpt_forpp.py --obs_save_path YOUR_OBS_SAVE_PATH
```
**(1).微调时产生的策略文件:$strategy_name_prefix**

**(2).merge成单个ckpt的策略文件:$src_obs_strategy_inference**

**(3).微调产生的多个模型分片位置:$ckpt_obs_path**


### 数据

##### 生成训练数据

推荐镜像环境[CPU镜像-mindspore_cpu.zip]，解压后docker load：https://openi.pcl.ac.cn/PengChengMind/PengChengMind-7B/datasets/

参考已制作数据集：https://openi.pcl.ac.cn/PengChengMind/PengChengMind-7B/datasets/pretrain_2B.zip

参考脚本：`/tools/pre_process_data.py`

在 YOUR_DATASET_PATH 目录下存放多个 `xxx.json` 文件，如果训练数据较多，最好每个 `json` 文件大小统一，且分开多个 `json` 存放，
大小可以 1M 一个文件。如果有繁体文字，需要转成简体，可以使用`zhconv`。

每个 `json` 文本格式为（需要换行符号分割不同样本）：
```txt
{"text": "sample1"}
{"text": "sample2"}
{"text": "sample2"}
```

```bash
python pre_process_data.py --input_glob "YOUR_DATASET_PATH/*.json" --output_file "YOUR_OUTPUT_PATH/mindrecord" --SEQ_LEN 4097
```

将会在YOUR_OUTPUT_PATH目录下生成mindrecord* 文件。


### NPU 模型转 GPU
转换流程：

一、merged-NPU模型文件，转NPY中间文件

```bash
python tools/check_merged_ckpt_7B.py
```
**(1).merge后单个NPU-ckpt文件路径：$ckpt_full_name**

**(2).转换出NPY文件路径：$ckpt_obs_path**

二、NPY中间文件，通过torch库，执行以下转换脚本，转换生成GPU模型

```bash
python tools/convert_ms_to_megatron.py
```
**(1).中间NPY文件路径：$ms_npy_base_path/merged_ckpt.npy**

**(2).输出GPU模型：$pt_save_path/model_optim_rng.pt**

### 声明
[鹏城·脑海模型开源协议](./docs/鹏城·脑海模型开源协议.pdf)

[数据集/算力来源声明]()
