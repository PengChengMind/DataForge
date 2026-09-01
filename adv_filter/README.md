In the LLM training process, data quality has a decisive impact on model performance. Advertisements as noise data in training corpora significantly reduce the quality and professionalism of the generated text. This filter effectively identifies and removes advertising content through a systematic cleaning process, ensuring the purity of training data.

Hazards of Advertisements in Model Training
1. Semantic Pollution
Repetitive pollution: High-frequency repetition of advertising phrases (e.g., "limited-time offer", "buy now") causes the model to over-learn commercial expression patterns.
Semantic shift: Promotional language distorts the original semantic distribution of words, affecting the model's understanding of normal contexts.

2. Failure of Knowledge Distillation
Information density dilution: Advertising content has extremely low information entropy, reducing the overall knowledge concentration of training data.
Logical structure disruption: The jump-cut logic of advertising copy disrupts the model's learning of reasonable argumentation chains.

### Training Data
* Training set: 179,000 samples, with approximately 107,000 positive samples and 72,000 negative samples;
* The dataset has undergone the following preprocessing steps:
  (1) Tokenization: splitting text into words or subwords;
  (2) Normalization: converting text to lowercase, removing punctuation, and handling special characters.
  (3) Stopword removal: deleting common words that contribute little to model performance.
  (4) Stemming/Lemmatization: reducing words to their root forms.
* Model evaluation: cross-validation shows the model prediction accuracy is 97.4%.

### Quick Experience
#### 1. Download and Install
* Refer to zrequirements.txt for program runtime environment configuration
```
   python 3.8, torchtext 0.13.1, torch 1.12.1
```
#### 2. Download Pretrained Weights
```
    wget -O model.bin https://openi.pcl.ac.cn/PengChengMind-Corpora/DataForge/src/branch/master/adv_filter/model/best_model.pt
```

#### 3. Train Model
python trainmodel.py

#### 3. Streaming Service (Optional)
python apicall.py --port 8080 --workers 4

For any questions, please submit an Issue or email zhangy03@pcl.ac.cn.


