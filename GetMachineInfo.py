import sqlalchemy
import pandas as pd
import json
import warnings
import requests
from mysqlengine import engine
warnings.filterwarnings("ignore")

#从数据库提取Token值到headers
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
        str(ItemNumber) + '/info'
    print(url)
    response = requests.request("GET", url, headers=headers)
    #获取返回Json,传入df2暂存.
    machineinfo = response.json()
    df2 = pd.DataFrame(machineinfo, columns=['machineItemNumber', 'machineId', 'machineName', 'machineCompany', 'machineSite', 'machineType',
                                             'machineModel', 'machineLatitude', 'machineLongitude', 'machineTimeZone', 'machineLastContact', 'machineLastData'], index=[0], dtype=str)
    #转换df2中machineid格式为数字
    df2['machineId'] = pd.to_numeric(df2['machineId'])
    #转换df2中machineLastContact时间格式
    df2['machineLastContact'] = pd.to_datetime(
        df2['machineLastContact'], infer_datetime_format=True)
    #转换df2中machineLastData时间格式
    df2['machineLastData'] = pd.to_datetime(
        df2['machineLastData'], infer_datetime_format=True)
    #在df数据集中追加df记录
    df = df.append(df2, ignore_index=True)
print(df)
#写入df数据集到certiq_v2.info
df.to_sql('info', con=engine, index=False, if_exists='replace')
print('table updated')
