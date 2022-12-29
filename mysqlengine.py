import sqlalchemy
from sqlalchemy import create_engine

engine = sqlalchemy.create_engine('mysql+pymysql://root:root123@localhost:3306/certiq_v2')
params = {
    'username': 'admin@epiroc.com',
    'password': 'admin',
}