The PCMind English Quality Evaluation Large Model is trained based on the PCMind project and follows the Transformer autoregressive architecture. It has been pre-trained specifically for the scenario of English text quality evaluation. For detailed code, please refer to[PengCheng-Mind](https://openi.pcl.ac.cn/PengChengMind/PengChengMind-7B)

### Model Structure
| PCMind 7B       | Parameters     |
|:----------------|:-------|
| seq_length      | 4096   | 
| vocab_size      | 125952 | 
| embedding_size  | 4096   | 
| num_layers      | 32     | 
| num_heads       | 32     |

### Environment requirements

python >= 3.7.5, mindspore >= 2.1.

It is recommended to use the official docker image of mindspore. (http://mirrors.cn-central-221.ovaijisuan.com/mirrors.html)

| Hardware        | Operating System  | Framework | Number of device required for 7B Inference | Number of device required for 7B Fine-tuning |
|:-----------|:------------------|:----------|:-------------------------------------------|:---------------------------------------------|
| Ascend 910 | EulerOS-aarch64   | MindSpore | 1 card                                     | ≥16 card                                     |  

### Download
[PCMind English Quality Evaluation Model Download](https://openi.pcl.ac.cn/PengChengMind-Corpora/DataForge/src/branch/master/En_quality_EvaluateTool/models/PC_Mind_350M-30000.ckpt)

### Usage
Please refer to [PengChengMind-7B](https://openi.pcl.ac.cn/PengChengMind/PengChengMind-7B)

#### PCMind-7B Model Inference
Startup command (--offline 1 indicates using NPU bare metal; modify local_ckpt_path in the script to the Pengcheng Mind 7B pre-trained model file path; --offline 0 indicates using the ModelArts environment; modify gpu_model_npy_obspath in the script to the Pengcheng Mind 7B pre-trained model file path;):
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


### NPU Model to GPU Conversion
Conversion Process:

1. Convert merged-NPU model files to NPY intermediate files

```bash
python tools/check_merged_ckpt_7B.py
```
**(1).Path of merged NPU-ckpt file：$ckpt_full_name**

**(2).Path of conveted NPY file：$ckpt_obs_path**

2. Use the torch library to run the following conversion script, converting the NPY intermediate files into a GPU model.

```bash
python tools/convert_ms_to_megatron.py
```
**(1).Path of intermediate NPY file：$ms_npy_base_path/merged_ckpt.npy**

**(2).Output GPU Model：$pt_save_path/model_optim_rng.pt**

### Declaration
[PCMind Model Open Source License](./docs/鹏城·脑海模型开源协议.pdf)

[Data Set/Computing Resource Statement]()
