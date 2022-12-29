import sqlalchemy
from sqlalchemy import create_engine

#change mysql connection setting.
engine = sqlalchemy.create_engine('mysql+pymysql://root:root123@localhost:3306/certiq_v2')

#change to you Certiq access information
params = {
    'username': 'admin@epiroc.com',
    'password': 'admin',
}
