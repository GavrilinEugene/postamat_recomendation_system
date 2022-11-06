#Загрузка данных по проходимости по Москве

import pandas as pd
from sqlalchemy import create_engine 
import h3
from shapely.geometry import Point
import geopandas as gpd
from enums import postgress_connection, postgress_db, lp, schema, RES
from utils import psql_insert_copy

engine = create_engine(f'postgresql://{lp}@{postgress_connection}/{postgress_db}')
    
df = pd.read_csv('../data/msk_pass.csv')
df = df.drop(columns = ['Unnamed: 0','Unnamed: 0.1'])


df.to_sql(schema=schema,name = 'msk_pass',\
                con = engine,if_exists = 'replace'\
                                      ,index = False,method = psql_insert_copy )
print("table msk_pass created")