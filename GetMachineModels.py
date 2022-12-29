import sqlalchemy
import pandas as pd
import json
import requests
from mysqlengine import engine

# 从数据库获取Token表
token = pd.read_sql('select userCode from token', con=engine)

# 提取Token值到headers
token = pd.DataFrame(token, columns=['userCode']).iloc[0, 0]
headers = {
    'Accept': 'application/json',
    'X-Auth-Token': token,
}

#发起GET请求
url = 'https://certiq-api.epiroc.com/v2/machineModels'
response = requests.request("GET", url, headers=headers)
print(response.url)
#获取返回json作为设备类型列表,传入DataFrame，写入Certiq_v2.machinemodels
machines = response.json()
df = pd.DataFrame(machines, dtype='str')
print(df)
df.to_sql(name='machinemodels', con=engine, index=False, if_exists='replace')
