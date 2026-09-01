 鹏城·脑海英文质量评估大模型是基于PengCheng-Mind项目完成训练，模型沿用Transformer自回归架构。针对英文文本质量评估场景进行预训练。详细代码请参见[PengCheng-Mind](https://openi.pcl.ac.cn/PengChengMind/PengChengMind-7B)

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

### 英文质量评估模型下载
[鹏城·脑海英文质量评估模型下载](https://openi.pcl.ac.cn/PengChengMind-Corpora/DataForge/src/branch/master/En_quality_EvaluateTool/models/PC_Mind_350M-30000.ckpt)

### 使用方式
请参照[脑海模型](https://openi.pcl.ac.cn/PengChengMind/PengChengMind-7B)项目

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
