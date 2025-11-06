from lmppl_cal.ppl_recurrent_gpt2 import Text_Extractor, PPL_Cal
import argparse
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-F", "--folder", type=str, required=True, help="目标文件夹")
    parser.add_argument("-K", "--keyword", type=str, required=False, help="关键词文件")
    parser.add_argument("-D", "--dest", type=str, required=True, help="输出文件")
    parser.add_argument("--ppl", action="store_true", help="是否计算ppl")
    parser.add_argument("--ppl-model", type=str, required=False, default="uer/gpt2-chinese-cluecorpussmall",
                        help="计算ppl的模型")
    parser.add_argument("--ppl-device", type=str, required=True, default="cuda", help="计算ppl用的device, cpu or cuda")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    calc: PPL_Cal = None
    if args.ppl:
        calc = PPL_Cal(args.ppl_model, args.ppl_device)
    et = Text_Extractor(args.folder,  args.dest, calc)
    et.run()
