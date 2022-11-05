#Сбор полигонов административных округов и районов Москвы

import os
import geopandas as gpd
from sqlalchemy import create_engine 
from geoalchemy2 import Geometry
from enums import postgress_connection, postgress_db, lp, schema, RES
engine = create_engine(f'postgresql://{lp}@{postgress_connection}/{postgress_db}')

#Округа
    
df_adm = gpd.read_file("../data/admin_level_5.shp",encoding = '1251')
df_msk = df_adm[df_adm['addr_regio']=='Москва']
df_msk = df_msk[~df_msk['ref'].isin(['ЗелАО','ТАО','НАО'])]
df_msk = df_msk[['name','geometry']].rename(columns = {'name':'okrug_name'})
df_msk['geometry'] = df_msk['geometry'].astype(str)

df_msk.to_sql(schema=schema,name = 'adm_okr_zones_temp',\
                    con = engine,if_exists = 'replace'\
                                          ,index = False)
engine.execute(f"drop table if exists {schema}.adm_okr_zones")
engine.execute(f"""
 create table {schema}.adm_okr_zones as
 select okrug_name,ST_SetSRID(ST_GeomFromText(geometry),4326) as geometry  from {schema}.adm_okr_zones_temp
""")
engine.execute(f"drop table if exists {schema}.adm_okr_zones_temp")


#Районы

df_adm = gpd.read_file("../data/admzones2021/admzones2021.shp")
df_adm = df_adm[['adm_name', 'okrug_name', 'sub_ter', 'geometry']]
df_adm = gpd.GeoDataFrame(df_adm)
df_msk = df_adm[df_adm['sub_ter']=='Старая Москва']
df_msk[['okrug_name','adm_name','geometry']].to_postgis(schema=schema,name = 'adm_zones',\
                    con = engine,if_exists = 'replace',dtype={'geometry': Geometry(geometry_type='Polygon', srid= 4326)}\
                                          ,index = False)
