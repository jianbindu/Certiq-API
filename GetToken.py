import pandas as pd
import requests
import sqlalchemy
from mysqlengine import engine
from mysqlengine import params

#设定Requests，URL和params

response = requests.get('https://certiq-api.epiroc.com/authentication/login', params=params)
#获取返回Json
json = response.json()
#传递Json到DataFrame
df = pd.DataFrame(json,index=['0'],columns=['userCode', 'expires'], dtype='str')
#更新userCode和expires到Certiq_v2.token
df.to_sql(name='token',con=engine,index=False,if_exists='replace')
print(df)
print('successed')