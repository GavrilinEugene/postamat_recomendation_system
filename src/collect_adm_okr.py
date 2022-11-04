#Сбор полигонов административных округов и районов Москвы

import os
import geopandas as gpd
from sqlalchemy import create_engine 
from geoalchemy2 import Geometry
import argparse

def run(args):
    
    login = args.login
    password = args.password
    host = args.serv
    port  = args.port
    
    engine = create_engine(f'postgresql://{login}:{password}@{host}:{port}/geo')

    
    #Округа
    
    df_adm = gpd.read_file("../data/admin_level_5.shp",encoding = '1251')
    df_msk = df_adm[df_adm['addr_regio']=='Москва']
    df_msk = df_msk[~df_msk['ref'].isin(['ЗелАО','ТАО','НАО'])]
    df_msk = df_msk[['name','geometry']].rename(columns = {'name':'okrug_name'})
    df_msk['geometry'] = df_msk['geometry'].astype(str)

    df_msk.to_sql(schema='postamat',name = 'adm_okr_zones_temp',\
                    con = engine,if_exists = 'replace'\
                                          ,index = False)
    engine.execute("drop table if exists postamat.adm_okr_zones")
    engine.execute("""
    create table postamat.adm_okr_zones as
    select okrug_name,ST_SetSRID(ST_GeomFromText(geometry),4326) as geometry  from postamat.adm_okr_zones_temp
    """)
    engine.execute("drop table if exists postamat.adm_okr_zones_temp")


    #Районы

    df_adm = gpd.read_file("../data/admzones2021/admzones2021.shp")
    df_adm = df_adm[['adm_name', 'okrug_name', 'sub_ter', 'geometry']]
    df_adm = gpd.GeoDataFrame(df_adm)
    df_msk = df_adm[df_adm['sub_ter']=='Старая Москва']
    df_msk[['okrug_name','adm_name','geometry']].to_postgis(schema='adhoc',name = 'adm_zones',\
                    con = engine,if_exists = 'replace',dtype={'geometry': Geometry(geometry_type='Polygon', srid= 4326)}\
                                          ,index = False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='desc')
    parser.add_argument('-l', '--login', default='')
    parser.add_argument('-p', '--password', default='')
    parser.add_argument('-s', '--serv', default='')
    parser.add_argument('-port', '--port', default='')
    args = parser.parse_args()
    run(args) 