### English Domain Classification Model

The filtered English sub-corpus is categorized into **38 domains** using a similar method. The English domain classification model is likewise obtained by fine-tuning a **BERT-base** model. Its training dataset contains **25,000** samples (validation set: **5,000** samples) and is constructed using the same procedure described above. The resulting model achieves a validation accuracy of **92%**.

* Training Corpus: 25,000 training samples  
  Validation Set Size: 5,000 samples
* Model Selection: BERT-base English text classification model (`bert-base-cased`)
* Model Training:
```
    python run.py --model bert --data_path /DATA_PATH/ 
```
* Model Inference:
```
    python run.py --model bert --data_path /DATA_PATH/ --result_data_path /RESULT_SAVE_PATH/
```

### Evaluation Results

Overall validation accuracy: **92.00%** (N=5000). Macro F1: **0.7893** | Weighted F1: **0.9151**.

Row-normalized confusion matrix (left) and per-class error matrix (right):

![English domain classification error analysis](./en_domain_metrics.png)

| Domain | n | P | R | F1 |
| --- | ---: | ---: | ---: | ---: |
| Physics | 154 | 0.961 | 0.948 | 0.954 |
| Computer Science | 471 | 0.932 | 0.983 | 0.957 |
| Mathematics | 248 | 0.882 | 0.964 | 0.921 |
| Materials Science | 66 | 0.934 | 0.864 | 0.898 |
| Astronomy | 49 | 1.000 | 0.694 | 0.819 |
| Earth Science | 28 | 0.889 | 0.571 | 0.696 |
| Engineering and technology | 108 | 0.951 | 0.907 | 0.929 |
| Others | 531 | 0.919 | 0.981 | 0.949 |
| Language and literature | 295 | 0.920 | 0.969 | 0.944 |
| Law | 287 | 0.899 | 0.958 | 0.927 |
| Medicine and health | 462 | 0.899 | 0.985 | 0.940 |
| Environment studies and forestry | 33 | 0.893 | 0.758 | 0.820 |
| Communication technology | 11 | 1.000 | 0.455 | 0.625 |
| Transportation | 23 | 1.000 | 0.652 | 0.789 |
| Biology | 151 | 0.958 | 0.901 | 0.928 |
| Education | 74 | 0.921 | 0.784 | 0.847 |
| Economics | 35 | 0.903 | 0.800 | 0.848 |
| Entertainment and tourism | 509 | 0.926 | 0.978 | 0.951 |
| Religious Studies | 89 | 0.916 | 0.854 | 0.884 |
| Arts | 158 | 0.912 | 0.918 | 0.915 |
| Philosophy | 110 | 0.913 | 0.864 | 0.888 |
| Business | 217 | 0.920 | 0.949 | 0.934 |
| Geography | 77 | 0.919 | 0.883 | 0.901 |
| Agriculture | 41 | 0.935 | 0.707 | 0.806 |
| Political Science | 232 | 0.937 | 0.966 | 0.951 |
| History | 238 | 0.925 | 0.937 | 0.931 |
| Psychology | 31 | 0.880 | 0.710 | 0.786 |
| Sociology | 18 | 0.733 | 0.611 | 0.667 |
| Architecture and design | 40 | 0.958 | 0.575 | 0.719 |
| Human physical performance and recreation | 27 | 0.933 | 0.519 | 0.667 |
| Military science | 8 | 0.000 | 0.000 | 0.000 |
| Food science | 29 | 0.944 | 0.586 | 0.723 |
| Information and communication sciences | 70 | 0.908 | 0.843 | 0.874 |
| Management Science | 15 | 0.857 | 0.400 | 0.545 |
| Archaeology | 13 | 1.000 | 0.538 | 0.700 |
| Energy science | 17 | 1.000 | 0.294 | 0.455 |
| Chemistry | 27 | 1.000 | 0.519 | 0.683 |
| Aerography | 8 | 1.000 | 0.125 | 0.222 |

### English Text Data Domain Classification System

Referencing the "Library of Congress Classification" and the "English Academic Discipline Classification Method" and combining with the current data status, a system of 38 English text data domains has been established, including: "Physics", "Computer Science", "Mathematics", "Materials Science", "Astronomy", "Earth Science", "Engineering and technology", "Others", "Language and literature", "Law", "Medicine and health", "Environment studies and forestry", "Communication technology", "Transportation", "Biology", "Education", "Economics", "Entertainment and tourism", "Religious Studies", "Arts", "Philosophy", "Business", "Geography", "Agriculture", "Political Science", "History", "Psychology", "Sociology", "Architecture and design", "Human physical performance and recreation", "Military science", "Food science", "Information and communication sciences", "Management Science", "Archaeology", "Energy science", "Chemistry", "Aerography".

### Data Domain Distribution

Please refer to [PCMind_en_dataset](https://openi.pcl.ac.cn/PengChengMind-Corpora/PCMind_en_dataset)
