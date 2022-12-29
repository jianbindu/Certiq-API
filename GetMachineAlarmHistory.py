import datetime
import json
import warnings

import pandas as pd
import requests
import sqlalchemy
warnings.filterwarnings("ignore")
from mysqlengine import engine

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
    start = datetime.datetime.now()-datetime.timedelta(days=30)
    start=start.strftime('%Y-%m-%d')
    end = datetime.datetime.now().strftime('%Y-%m-%d')
    url = 'https://certiq-api.epiroc.com/v2/machines/' + \
        str(ItemNumber) + '/alarmHistory?'+'start='+str(start)+'&end='+str(end)
    print('Get Machine Alarm History: '+url)
    response = requests.request("GET", url, headers=headers)
    # 获取返回Json,传入df2暂存.
    machineinfo = response.json().get('data')
    df2 = pd.DataFrame(machineinfo, columns=['alarmId', 'alarmName', 'alarmDescription', 'alarmNodeIndex', 'alarmLevel', 'alarmTime',
                                            'alarmValue', 'alarmAcknowledgedBy'],dtype=str)
    #向df2数据集中加入列ItemNumber
    df2['ItemNumber']=ItemNumber
    #转换df2中alarmTime为datatime格式
    df2['alarmTime'] = pd.to_datetime(
        df2['alarmTime'], infer_datetime_format=True)
    df = df.append(df2, ignore_index=True)
#写入df到Certiq_v2.alarmhistory中
df.to_sql(name='alarmhistory',con=engine, index=False, if_exists='append')

#清洗重复数据
alarmhistory=pd.read_sql('select * from alarmhistory', con=engine)
df=pd.DataFrame(alarmhistory)
df.drop_duplicates(inplace=True)
df.to_sql(name='alarmhistory',con=engine,index=False,if_exists='replace')
print('GetMachineAlarmHistory: Done')
