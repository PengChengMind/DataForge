import re
import argparse
import json
import torch

import os
import json
os.environ["CUDA_VISIBLE_DEVICES"] = "0"
class PPL_Cal:
    def __init__(self, model_id, device):
        from transformers import GPT2LMHeadModel, AutoTokenizer  # pylint: disable=import-outside-toplevel
        self.device = device
        self.tokenizer = AutoTokenizer.from_pretrained(
            "/userhome/zhangy03/lmppl-main/test/gpt2-chinese-cluecorpussmall")
        self.model = GPT2LMHeadModel.from_pretrained(
            "/userhome/zhangy03/lmppl-main/test/gpt2-chinese-cluecorpussmall").to(self.device)
        # self.tokenizer = AutoTokenizer.from_pretrained("uer/gpt2-distil-chinese-cluecorpussmall")
        # self.model = GPT2LMHeadModel.from_pretrained("uer/gpt2-distil-chinese-cluecorpussmall").to(self.device)

    def calc(self, text):
        # reference: https://huggingface.co/docs/transformers/perplexity
        encodings = self.tokenizer(text, return_tensors="pt")
        max_length = self.model.config.n_positions
        stride = 512
        seq_len = encodings.input_ids.size(1)

        nlls = []
        prev_end_loc = 0
        for begin_loc in range(0, seq_len, stride):
            end_loc = min(begin_loc + max_length, seq_len)
            trg_len = (
                    end_loc - prev_end_loc
            )  # may be different from stride on last loop
            input_ids = encodings.input_ids[:, begin_loc:end_loc].to(self.device)
            target_ids = input_ids.clone()
            target_ids[:, :-trg_len] = -100

            with torch.no_grad():
                outputs = self.model(input_ids, labels=target_ids)
                neg_log_likelihood = outputs.loss
            nlls.append(neg_log_likelihood)
            prev_end_loc = end_loc
            if end_loc == seq_len:
                break

        ppl = torch.exp(torch.stack(nlls).mean())  # pylint: disable=no-member
        return float(ppl)


class Text_Extractor:
    def __init__(self, folder: str, outputfile: str, ppl: PPL_Cal, mode: int = 2):
        self.file = folder
        self.ppl = ppl
        self.sets = set()
        self.outwriter = open(outputfile, "w", encoding="utf-8")

    def __del__(self):
        try:
            self.outwriter.close()
        except Exception as e:
            print(e.args)


    def record(self, ret):
        self.outwriter.write(json.dumps(ret, ensure_ascii=False) + '\n\n')

    def extract(self, file):
        with open(file, "r", encoding="utf-8", errors='ignore') as fl:
            lines = fl.readlines()
            for raw_line in lines:
                try:
                    j_data = json.loads(raw_line)
                    ret = {
                        "file": file,
                        "content": j_data['text'],
                        "src":'source_text',
                        "ppl": self.ppl.calc(j_data['text']) if self.ppl else 0,
                    }
                    self.record(ret)
                except json.JSONDecodeError as e:
                    print("Error: JSON string format error")
                    print(e.msg)
                    print(e.doc)
                except TypeError:
                    print("Error: The parameter is not a string")
                except ValueError as e:
                    print("Error: Unsupported data type")
                    print(e)
                else:
                    continue



    def run(self):
        try:
            self.extract(self.file)
        except Exception:
            pass