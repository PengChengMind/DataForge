#!/bin/bash

find /mgData1/zhangjiayuan/en_dataset_access/process/process_result/pile_RedPajama_arxiv/ -type f -name "*_bad.json" -exec cat {} + > /mgData1/zhangjiayuan/en_dataset_access/result/pile_RedPajama_arxiv/pile_RedPajama_arxiv_bad.json

find /mgData1/zhangjiayuan/en_dataset_access/process/process_result/pile_RedPajama_arxiv/ -type f -name "*_clean.json" -exec cat {} + > /mgData1/zhangjiayuan/en_dataset_access/result/pile_RedPajama_arxiv/pile_RedPajama_arxiv_clean.json
