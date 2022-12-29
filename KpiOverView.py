import datetime
import json
import warnings

import pandas as pd
import requests
import sqlalchemy

from mysqlengine import engine
warnings.filterwarnings("ignore")
# 从数据库提取Token值到headers
token = pd.read_sql('select userCode from token', con=engine)
token = pd.DataFrame(token, columns=['userCode']).iloc[0, 0]
headers = {
    'Accept': 'application/json',
    'X-Auth-Token': token,
}
# 统计设备总数作为变量MaxMachineInfo
MachineAmount = pd.read_sql(
    'select machineItemNumber from machines', con=engine)
MaxMachineIndex = (MachineAmount.shape[0])
df = pd.DataFrame([])
# 通过MaxMachineIndex循环读取ItemNumber,并传递给URL
for num in range(0, MaxMachineIndex):
    ItemNumber = pd.DataFrame(MachineAmount, columns=[
                              'machineItemNumber']).iloc[num, 0]
    url = 'https://certiq-api.epiroc.com/v2/machines/' + \
        str(ItemNumber) + '/kpiOverview'
    print(url)
    response = requests.request("GET", url, headers=headers)
    # 获取返回Json,传入df2暂存.
    machineinfo = response.json()
    df2 = pd.DataFrame(machineinfo, columns=['name', 'nodeIndex', 'value', 'timeStamp'],dtype=str)
    #向df2数据集中加入列ItemNumber
    df2['ItemNumber']=ItemNumber
    #转换df2中alarmTime为datatime格式
    df2['timeStamp'] = pd.to_datetime(
        df2['timeStamp'], infer_datetime_format=True)
    df = df.append(df2, ignore_index=True)
#写入df到Certiq_v2.alarmstatus中
df.to_sql(name='kpioverview',con=engine, index=False, if_exists='replace')
print(df)
