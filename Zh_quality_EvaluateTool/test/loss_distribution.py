
import matplotlib.pyplot as plt
import numpy as np
import json

file_path = '../sample_test/test_distrubution.json'
loss_distributer = []

#画loss曲线分布图
def loss_distributer(file_path):
    x = np.linspace(1, 3000, 3000)
    with open(file_path, 'r', encoding="utf-8",errors='ignore') as fcc_file:
        for line in fcc_file:
            if line.strip() == "":
                continue
            if line in ['\n', '\r\n']:
                continue
            if  line :
                fcc_data = json.loads(line)
                loss_distributer.append(float(fcc_data['ppl']))

    plt.ylim(0, 100)
    plt.title('loss分布')
    plt.xlabel('x')
    plt.ylabel('y')
    plt.plot(x, loss_distributer, color='r')
    plt.show()




import argparse
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-F", "--filename", type=str, required=True, help="文件")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    loss_distributer(args.filename)







