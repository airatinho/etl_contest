import json
import pandas as pd
from sqlalchemy import create_engine

#connect params
config = json.load(open("etl/config.json", encoding='utf-8'))
host=config['host']
port_from=config['port_from']
user= config['user']
password= config['password']
db=config['db']
port_to=config['port_to']


#connection
engine_from = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port_from}/{db}')
engine_to = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port_to}/{db}')


#queries
querry_to_check="""SELECT * from transactions_denormalized"""

querry="""SELECT
       tr.id as id,
       tr.dt as dt,
       tr.idoper as idoper,
       tr.move as move,
        tr.amount as amount,
       ot.name as name_oper

from transactions tr
left join operation_types ot
on ot.id=tr.idoper
order by dt asc
"""
querry_max_date_to="""SELECT max(dt) as dt from transactions_denormalized"""
querry_max_date_from="""SELECT max(dt) as dt from transactions"""


if pd.read_sql(querry_to_check,engine_to).empty:
    df=pd.read_sql(querry,engine_from) #если данных много юзаем chunksize - чтение по батчам
    for uniq_dat in df['dt'].unique(): # загрузка по одному часу - учитываем также дату
        df[df['dt'] == uniq_dat].to_sql('transactions_denormalized', engine_to, index=False, if_exists='append')

else:
    max_loaded_to=pd.read_sql(querry_max_date_to,engine_to)['dt'][0]
    max_loaded_from = pd.read_sql(querry_max_date_from, engine_from)['dt'][0]
    df = pd.read_sql(querry, engine_from)  # если данных много юзаем chunksize - чтение по батчам
    if max_loaded_to!=max_loaded_from:
        df[df['dt'] > pd.to_datetime(max_loaded_to)].to_sql('transactions_denormalized', engine_to, index=False, if_exists='append')

