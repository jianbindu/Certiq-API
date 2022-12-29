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
params = {
    'company': 'Kamoa Copper Mine',
}

# 发起GET请求
url = 'https://certiq-api.epiroc.com/v2/machines'
response = requests.request("GET", url, headers=headers, params=params)
print(response.url)
# 获取返回json作为设备列表,传入DataFrame，写入Certiq_v2.machines
machines = response.json().get('data')
df = pd.DataFrame(machines, columns=['machineItemNumber', 'machineId', 'machineName', 'machineCustomerCenter', 'machineCompany',
                  'machineSite', 'machineType', 'machineModel', 'rigConfigVersion', 'rigSoftwareVersion'], dtype='str')
df.to_sql(name='machines', con=engine, index=False, if_exists='replace')

print(df)
