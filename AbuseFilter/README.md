# Large Language Model Corpus Safety Filtering Service

During the construction and maintenance of large language models, ensuring the **safety** and **compliance** of the corpus is of critical importance. This involves not only data cleaning and quality control, but also fine-grained ideological filtering to avoid potential biases and inappropriate content. The data cleaning process described earlier—including removal of irrelevant content, duplicate samples, and incoherent text—lays the foundation for initial corpus preprocessing. However, to further enhance corpus safety and compliance, more sophisticated filtering methods are required. To address this need, we have developed a **Corpus Safety Filtering Service for Large Language Models**. This service employs a **model-based filtering approach** with a **hierarchical filtering strategy**, progressively improving data quality and recall during the cleaning pipeline to achieve efficient and precise corpus screening.

---

## Training Data

- **Training Set**: 180,000 samples, including Positive samples: ~104,000 and Negative samples: ~76,000  

- **Preprocessing Steps**:
  1. **Tokenization**: Splitting text into words or subwords.
  2. **Normalization**: Converting text to lowercase, removing punctuation, and handling special characters.
  3. **Stopword Removal**: Removing common words that contribute little to model performance.
  4. **Stemming/Lemmatization**: Reducing words to their root forms.

- **Model Evaluation**:  
  Evaluated using cross-validation. The model achieves a prediction accuracy of **98.7%**.

---

## Quick Start Guide

### 1. Download and Install Dependencies

- Refer to `zrequirements.txt` for environment configuration:
  ```bash
  pip install -r zrequirements.txt

#### 2.Download Pretrained Model Weights
```
    wget -O model.bin https://openi.pcl.ac.cn/PengChengMind-Corpora/DataForge/src/branch/master/AbuseFilter/model.pt
```

#### 3. Run as a Streaming Service (Optional)
python apicall.py --port 8080 --workers 4


For any questions, please submit an Issue or email zhangy03@pcl.ac.cn.