import json
import pandas as pd
from sqlalchemy import create_engine
import unittest
import os

#connect params

config = json.load(open("config.json", encoding='utf-8'))
host=config['host']
port_from=config['port_from']
user= config['user']
password= config['password']
db=config['db']
port_to=config['port_to']


#connection
engine_from = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port_from}/{db}')
engine_to = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port_to}/{db}')



querry_from="""SELECT
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
querry_to="""SELECT * from transactions_denormalized"""


class TestCase(unittest.TestCase):
    """Simple equality test"""

    df_from=pd.read_sql(querry_from,engine_from)
    df_to=pd.read_sql(querry_to,engine_to)

    assert df_from.equals(df_to)