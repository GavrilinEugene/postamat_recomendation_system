#для загрузки данных с портала открытых данных Москвы

import pandas as pd
import h3
import requests
import json
from sqlalchemy import create_engine        
import csv
from io import StringIO
from shapely.geometry import Point
import geopandas as gpd
import numpy as np
from enums import postgress_connection, postgress_db, lp, schema, RES,api_key
from utils import psql_insert_copy
engine = create_engine(f'postgresql://{lp}@{postgress_connection}/{postgress_db}')
API_KEY = api_key

        
def split_type1(x):
    """
    Прменяется для отсечения наименования района в адресе (Для киосков печати)
    """
    try:
        result = str(x).split(',',1)[1]
    except:
        result = ''
    return result
        
def get_dataframe_type1(API_KEY,dataset_id,purpose_name):
    """
    Для киосков печати
    : param API_KEY: API key for apidata.mos.ru
    : param dataset_id: id of dataset from data.mos.ru
    : param purpose_name: categorie of objects to use on next steps
    returns: pd.DataFrame with all objects from dataset with parameters
    """
    req_text = f"https://apidata.mos.ru/v1/datasets/{dataset_id}/features?api_key={API_KEY}"
    df= pd.read_json(req_text)
    df['lon'] = df['features'].apply(lambda x: x['geometry']['coordinates'][0])
    df['lat'] = df['features'].apply(lambda x: x['geometry']['coordinates'][1])
    df['name'] = df['features'].apply(lambda x: x['properties']['Attributes']['Name'])

    df['address'] = df['features'].apply(lambda x: x['properties']['Attributes']['Address'])
    df['Specialization'] = df['features'].apply(lambda x: x['properties']['Attributes']['Specialization'])

    df['ContractStatus'] = df['features'].apply(lambda x: x['properties']['Attributes']['ContractStatus'])
    df['ContractEnd'] = df['features'].apply(lambda x: x['properties']['Attributes']['ContractEnd'])
    df['id'] = df['features'].apply(lambda x: x['properties']['Attributes']['global_id'])
    df = df.drop(columns = ['features','type'])
    df['geo_h3_10'] = df.apply(lambda x: h3.geo_to_h3(x['lat'],x['lon'],10),axis=1)
    
    df = df[df['ContractStatus']=='действует']
    
    df['floors_ground_count'] = 1


    df['geometry_centroid'] = df.apply(lambda x: Point(x['lon'],x['lat']),axis = 1)
    df['geometry']  = df['geometry_centroid'] 
    df['purpose_name'] = purpose_name#df['Specialization'] 
    df['structure_info_material'] = np.nan
    df['structure_info_apartments_count'] = np.nan
    df = df[['floors_ground_count', 'geometry_centroid', 'geometry', 'id', 'name','address',
       'purpose_name', 'structure_info_material',
       'structure_info_apartments_count', 'lat', 'lon', 'geo_h3_10']]
    df['address'] = df['address'].fillna("-")
    df['address'] = df['address'].\
    str.replace("Российская Федерация, город Москва, внутригородская территория муниципальный округ","")

    df['address'] = df['address'].apply(split_type1)

    
    return df

def get_dataframe_type2(API_KEY,dataset_id,purpose_name):
    """
    Для библиотек и домов культуры
    : param API_KEY: API key for apidata.mos.ru
    : param dataset_id: id of dataset from data.mos.ru
    : param purpose_name: categorie of objects to use on next steps
    returns: pd.DataFrame with all objects from dataset with parameters
    """
    
    req_text = f"https://apidata.mos.ru/v1/datasets/{dataset_id}/features?api_key={API_KEY}"
    df= pd.read_json(req_text)
    df['lon'] = df['features'].apply(lambda x: x['geometry']['coordinates'][0][0])
    df['lat'] = df['features'].apply(lambda x: x['geometry']['coordinates'][0][1])
    df['name'] = df['features'].apply(lambda x: x['properties']['Attributes']['CommonName'])
    df['id'] = df['features'].apply(lambda x: x['properties']['Attributes']['global_id'])
    df['address'] = df['features'].apply(lambda x: x['properties']['Attributes']['ObjectAddress'])
    df = df.drop(columns = ['features','type'])
    df['geo_h3_10'] = df.apply(lambda x: h3.geo_to_h3(x['lat'],x['lon'],10),axis=1)
    df['floors_ground_count'] = 1
    df['geometry_centroid'] = df.apply(lambda x: Point(x['lon'],x['lat']),axis = 1)
    df['geometry']  = df['geometry_centroid'] 
    df['purpose_name'] = purpose_name
    df['structure_info_material'] = np.nan
    df['structure_info_apartments_count'] = np.nan
    df = df[['floors_ground_count', 'geometry_centroid', 'geometry', 'id', 'name','address',
       'purpose_name', 'structure_info_material',
       'structure_info_apartments_count', 'lat', 'lon', 'geo_h3_10']]
    
    
    return df   


def get_dataframe_type3(API_KEY,dataset_id,purpose_name):
    """
    Для мфц
    : param API_KEY: API key for apidata.mos.ru
    : param dataset_id: id of dataset from data.mos.ru
    : param purpose_name: categorie of objects to use on next steps
    returns: pd.DataFrame with all objects from dataset with parameters
    """
    req_text = f"https://apidata.mos.ru/v1/datasets/{dataset_id}/features?api_key={API_KEY}"
    df= pd.read_json(req_text)
    df['lon'] = df['features'].apply(lambda x: x['geometry']['coordinates'][0])
    df['lat'] = df['features'].apply(lambda x: x['geometry']['coordinates'][1])
    df['name'] = df['features'].apply(lambda x: x['properties']['Attributes']['CommonName'])
    df['id'] = df['features'].apply(lambda x: x['properties']['Attributes']['global_id'])
    df['address'] = df['features'].apply(lambda x: x['properties']['Attributes']['Address'])
    df = df.drop(columns = ['features','type'])
    df['geo_h3_10'] = df.apply(lambda x: h3.geo_to_h3(x['lat'],x['lon'],10),axis=1)
    df['floors_ground_count'] = 1
    df['geometry_centroid'] = df.apply(lambda x: Point(x['lon'],x['lat']),axis = 1)
    df['geometry']  = df['geometry_centroid'] 
    df['purpose_name'] = purpose_name
    df['structure_info_material'] = np.nan
    df['structure_info_apartments_count'] = np.nan
    df = df[['floors_ground_count', 'geometry_centroid', 'geometry', 'id', 'name','address',
       'purpose_name', 'structure_info_material',
       'structure_info_apartments_count', 'lat', 'lon', 'geo_h3_10']]
    
    df['address'] = df['address'].fillna("-")
    df['address'] = df['address'].\
    str.replace("Российская Федерация, город Москва, внутригородская территория муниципальный округ","")

    df['address'] = df['address'].apply(split_type1)

    return df

def get_address(x):
    try:
        result = x['properties']['Attributes']['OrgInfo'][0]['LegalAddress'].lower()
    except:
        result = np.nan
    return result

def split_type2(x):    
    try:
        result = str(x).split(',',2)[2]
    except:
        result = ''
   
    return result

def get_dataframe_type4(API_KEY,dataset_id,purpose_name):
    """
    Для спортивных объектов
    : param API_KEY: API key for apidata.mos.ru
    : param dataset_id: id of dataset from data.mos.ru
    : param purpose_name: categorie of objects to use on next steps
    returns: pd.DataFrame with all objects from dataset with parameters
    """
    req_text = f"https://apidata.mos.ru/v1/datasets/{dataset_id}/features?api_key={API_KEY}"
    df= pd.read_json(req_text)
    df['lon'] = df['features'].apply(lambda x: x['geometry']['coordinates'][0][0])
    df['lat'] = df['features'].apply(lambda x: x['geometry']['coordinates'][0][1])
    df['name'] = df['features'].apply(lambda x: x['properties']['Attributes']['CommonName'])
    df['id'] = df['features'].apply(lambda x: x['properties']['Attributes']['global_id'])
    df['address'] = df['features'].apply(get_address)#(lambda x: x['properties']['Attributes']['ObjectAddress'][0]['Address'])
    df = df.drop(columns = ['features','type'])
    df['geo_h3_10'] = df.apply(lambda x: h3.geo_to_h3(x['lat'],x['lon'],10),axis=1)
    df['floors_ground_count'] = 1
    df['geometry_centroid'] = df.apply(lambda x: Point(x['lon'],x['lat']),axis = 1)
    df['geometry']  = df['geometry_centroid'] 
    df['purpose_name'] = purpose_name
    df['structure_info_material'] = np.nan
    df['structure_info_apartments_count'] = np.nan
    df = df[['floors_ground_count', 'geometry_centroid', 'geometry', 'id', 'name','address',
       'purpose_name', 'structure_info_material',
       'structure_info_apartments_count', 'lat', 'lon', 'geo_h3_10']]
    df['address'] = df['address'].apply(split_type2)

    return df
    
df_sport = get_dataframe_type4(API_KEY=API_KEY,dataset_id = 629,purpose_name = 'спортивный объект')
df_kiosk_papers = get_dataframe_type1(API_KEY=API_KEY,dataset_id = 2781,purpose_name = 'киоск печати')
df_libr = get_dataframe_type2(API_KEY=API_KEY,dataset_id = 526,purpose_name = 'библиотека')
df_dk = get_dataframe_type2(API_KEY=API_KEY,dataset_id = 493,purpose_name = 'дом культуры')
df_mfc = get_dataframe_type3(API_KEY=API_KEY,dataset_id = 611,purpose_name = 'МФЦ')
    
df_all = pd.concat([df_kiosk_papers,df_libr,df_dk,df_mfc,df_sport])


good_objects = ['спортивный объект',\
                'библиотека','МФЦ','дом культуры','киоск печати']

df_all = df_all[df_all["purpose_name"].isin(good_objects)]

gdf_all = gpd.GeoDataFrame(df_all)

gdf_all = gdf_all.set_crs(4326)

gdf_all.to_postgis(schema=schema,name = 'obj_from_mos_api',\
                     con = engine,if_exists = 'replace')
    
    
engine.execute(f"drop table if exists {schema}.all_objects")
engine.execute(f"""
    create table {schema}.all_objects as
    select obj2.* from (
    select obj.*,ROW_NUMBER() OVER(PARTITION BY obj.id,obj.geometry order by obj.id)
    from (
        select 
            address as address_name,
            floors_ground_count,
            --geometry_centroid,
            geometry,
            cast(id as text),
            "name",
            purpose_name,
            cast(structure_info_material as text),
            structure_info_apartments_count,
            lat,
            lon,
            geo_h3_10 
        from {schema}.obj_from_mos_api
        union all
        (select  
            address_name,
            floors_ground_count,
            --geometry_centroid,
            geometry,
            cast(id as text),
            "name",
            purpose_name,
            cast(structure_info_material as text),
            structure_info_apartments_count,
            lat,
            lon,
            geo_h3_10 
        from {schema}.platform_buildings where purpose_name ='Жилой дом' or structure_info_apartments_count > 10 )	
        ) obj
    where purpose_name in ('спортивный объект','Жилой дом','киоск печати','дом культуры','МФЦ','библиотека','Малоэтажный жилой дом')
    )obj2 
    where  row_number = 1;

    update {schema}.all_objects set address_name='' where address_name='[]';
    update {schema}.all_objects set purpose_name='жилой дом' where purpose_name in ('Жилой дом','Малоэтажный жилой дом');
    GRANT SELECT 
    ON {schema}.all_objects 
    TO streamlit_app;
    """)
    
print(f"table with objects created")



