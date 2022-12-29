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
    start = datetime.datetime.now()-datetime.timedelta(days=30)
    start = start.strftime('%Y-%m-%d')
    end = datetime.datetime.now().strftime('%Y-%m-%d')
    url = 'https://certiq-api.epiroc.com/v2/machines/' + \
        str(ItemNumber) + '/kpis/daily?'+'start='+str(start)+'&end='+str(end)
    print('Get Machine Daily Kpi: '+url)
    response = requests.request("GET", url, headers=headers)
    # 获取返回Json,传入df2暂存.
    machineinfo = response.json()
    df2 = pd.DataFrame(machineinfo, columns=['date', 'dailyDrillHours', 'dailyDrillMeters', 'dailyDrillMetersPerEngineHour',
                                             'dailyDrillMetersPerDrillHour', 'dailyDrillHoursPerEngineHour', 'dailyDrillHoles', 'dailyFuelLiters', 'dailyFuelLitersPerHour', 'dailyFuelCO2Emission', 'dailyFuelLitersPerTonnes', 'dailyFuelLitersPerMeter', 'dailyLoadingTonnes', 'dailyLoadingTonnesPerHour', 'dailyLoadingNumberOfBuckets', 'dailyLoadingTonnesPerBucket', 'dailyLoadingNumberOfBoxes', 'dailyLoadingTonnesPerBox', 'dailyUtilizationAvailableHours', 'dailyUtilizationWorkedHours', 'dailyUtilizationDrillHours', 'dailyUtilizationHydraulicPumpHours', 'dailyUtilizationTrammingHours', 'dailyUtilizationIdleHours', 'dailyUtilizationEngineHours'], dtype=str)
    # 向df2数据集中加入列ItemNumber
    df2['ItemNumber'] = ItemNumber
    # 转换df2中data为datatime格式
    df2['date'] = pd.to_datetime(
        df2['date'], infer_datetime_format=True)
    df = df.append(df2, ignore_index=True)
# 写入df到Certiq_v2.dakpi中
df.to_sql(name='dailykpi', con=engine, index=False, if_exists='append')
#清洗重复数据
dailykpi=pd.read_sql('select * from dailykpi',con=engine)
df=pd.DataFrame(dailykpi)
df.drop_duplicates(inplace=True)
df.to_sql(name='dailykpi',con=engine,index=False,if_exists='replace')
print('GetMachineDailyKpi:Done')
