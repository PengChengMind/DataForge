import requests
import json
# 要传入的数据
input_text = {
				"texts": ['我是中国人'],
				"source_ids": [],	
				"platform": 0
			}
# 要监听的地址
url = 'http://127.0.0.1:8802/adclass'  
# 假设 Flask 应用监听在本地的 5000 端口上
response = requests.get(url, json = input_text)
if response.status_code == 200:
	processed_data = json.loads(response.content)
	print(processed_data)
else:print('请求失败：', response.status_code)