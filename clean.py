
import pandas as pd
import sqlalchemy

from mysqlengine import engine

servicestatus=pd.read_sql('SELECT * FROM alarmhistory',con=engine)
df=pd.DataFrame(servicestatus)
print(df.duplicated())