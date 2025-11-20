### English Domain Classification Model
* Training Corpus: 25,000 training samples  
  Validation Set Size: 5,000 samples

* Model Selection: BERT-based Chinese text classification model
* Model Training:
```
    python run.py --model bert --data_path /DATA_PATH/ 
```
* Model Inference:
```
    python run.py --model bert --data_path /DATA_PATH/ --result_data_path /RESULT_SAVE_PATH/
```




### English Text Data Domain Classification System
Referencing the "Library of Congress Classification" and the "English Academic Discipline Classification Method," and combining with the current data status, a system of 38 English text data domains has been established, including:"Physics", "Computer Science", "Mathematics", "Materials Science", "Astronomy", "Earth Science", "Engineering and technology", "Others", "Language and literature", "Law","Medicine and health", "Environment studies and forestry", "Communication technology", "Transportation", "Biology", "Education", "Economics","Entertainment and tourism", "Religious Studies", "Arts", "Philosophy", "Business", "Geography","Agriculture", "Political Science","History", "Psychology", "Sociology", "Architecture and design", "Human physical performance and recreation", "Military science","Food science", "Information and communication sciences",
 "Management Science", "Archaeology", "Energy science", "Chemistry", "Aerography".
### Data Domain Distribution
Please refer to [PCMind_en_dataset](https://openi.pcl.ac.cn/PengChengMind-Corpora/PCMind_en_dataset)

