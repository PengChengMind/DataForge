for i in {4..4}
do
    echo "$i"
    for j in {0..4}
    do
        echo "$j"
        if [ $i -eq $j ]
        then
            continue
        else
            cat data/sample_1_zy/split_file/chat_train_$j >> data/sample_1_zy/chat_train.json
        fi
    done
    mv data/sample_1_zy/chat_train.json data/sample_1_zy/train.json
    cp data/sample_1_zy/split_file/chat_train_$i data/sample_1_zy/dev.json
    cp data/sample_1_zy/split_file/chat_train_$i data/sample_1_zy/test.json
    python3.9 run.py  --model bert --data_path ./data/sample_1_zy/
    mv data/sample_1_zy/saved_dict data/sample_1_zy/saved_dict_0$i
    mkdir -p data/sample_1_zy/saved_dict
done
