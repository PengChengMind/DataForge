
import os
import logging
import gc
from typing import List
from tqdm import tqdm
import transformers
import torch

os.environ["OMP_NUM_THREADS"] = "1"  # to turn off warning message
os.environ["TOKENIZERS_PARALLELISM"] = "false"  # to turn off warning message
PAD_TOKEN_LABEL_ID = torch.nn.CrossEntropyLoss().ignore_index
FORCE_RESET = bool(int(os.getenv("FORCE_RESET", "0")))


class LM:
    """ Language Model. """

    def __init__(self,
                 model: str = 'gpt2',
                 max_length: int = None,
                 num_gpus: int = None,
                 device_map: str = None,
                 ):
        """ Language Model.
        @param model: Model alias or path to local model file.
        @param num_gpus: Number of gpus to be used.
        """

        self.tokenizer = transformers.AutoTokenizer.from_pretrained("/userhome/zhangy03/chatGlm-6B",trust_remote_code=True)
        # self.tokenizer = transformers.AutoTokenizer.from_pretrained(
        #     "THUDM/chatglm-6b",
        #     trust_remote_code=True
        # )
        self.model = transformers.AutoModel.from_pretrained("/userhome/zhangy03/chatGlm-6B",trust_remote_code=True).half().cuda()
        # self.model = (
        #     transformers.AutoModel.from_pretrained(cd ch
        #         "THUDM/chatglm-6b",
        #         trust_remote_code=True)
        #     .half()
        #     .cuda()
        # )

        self.model = self.model.half().cuda()
        self.model.eval()

        if self.tokenizer.pad_token is None:
            self.tokenizer.add_special_tokens({'pad_token': "<<PAD>>"})
            self.model.resize_token_embeddings(len(self.tokenizer))

        if max_length is None:
            self.max_length = None
        else:
            self.max_length = max_length if max_length is not None else self.tokenizer.model_max_length
            assert self.max_length <= self.tokenizer.model_max_length, f"{self.max_length} > {self.tokenizer.model_max_length}"

        # loss function
        self.loss_fct = torch.nn.CrossEntropyLoss(reduction='none')
        if device_map is None:
            num_gpus = torch.cuda.device_count() if num_gpus is None else num_gpus
            if num_gpus == 1:
                self.model.to('cuda')
                self.device = self.model.device
            elif num_gpus > 1:
                self.model = torch.nn.DataParallel(self.model)
                self.model.to('cuda')
                self.device = self.model.module.device
        self.model.half().eval()
        logging.info(f'\t * model is loaded on: {self.device}')

    def get_perplexity(self, input_texts: str or List, batch: int = None):
        """ Compute the perplexity on recurrent LM.

        :param input_texts: A string or list of input texts for the encoder.
        :param batch: Batch size
        :return: A value or list of perplexity.
        """

        # batch preparation
        single_input = type(input_texts) == str
        input_texts = [input_texts] if single_input else input_texts
        batch = len(input_texts) if batch is None else batch
        batch_id = list(range(0, len(input_texts), batch)) + [len(input_texts)]
        batch_id = list(zip(batch_id[:-1], batch_id[1:]))

        loss_list = []
        with torch.no_grad():
            for s, e in tqdm(batch_id):
                # run model inference
                if self.max_length is not None:
                    model_inputs = self.tokenizer(str(input_texts[s:e]), max_length=self.max_length, truncation=True, padding='max_length', return_tensors='pt')
                else:
                    model_inputs = self.tokenizer(str(input_texts[s:e]), truncation=True, padding=True, return_tensors='pt')
                if 'token_type_ids' in model_inputs:
                    model_inputs.pop('token_type_ids')

                output = self.model(**{k: v.to(self.device) for k, v in model_inputs.items()})

                label = model_inputs['input_ids']
                label[label == self.tokenizer.pad_token_id] = PAD_TOKEN_LABEL_ID
                label = torch.concat([label[:, 1:], torch.tensor([[PAD_TOKEN_LABEL_ID] * label.shape[0]]).T], dim=1).to(self.device)

                shift_logits = output['logits'][..., :-1, :].contiguous()
                shift_labels = label[..., 1:].contiguous()

                # compute loss
                valid_length = (label != PAD_TOKEN_LABEL_ID).sum(dim=-1)
                loss = self.loss_fct(shift_logits.view(-1, shift_logits.size(-1)), shift_labels.view(-1))
                loss = loss.view(len(output['logits']), -1)
                loss = torch.sum(loss, -1) / valid_length
                loss_list += loss.cpu().tolist()

                if FORCE_RESET:
                    del model_inputs
                    del loss
                    del output
                    gc.collect()
                    torch.cuda.empty_cache()

        # conversion to perplexity
        ppl = [(i) for i in loss_list]
        return ppl[0] if single_input else ppl
