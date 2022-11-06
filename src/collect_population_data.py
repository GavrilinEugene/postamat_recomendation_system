#Загрузка данных по населению Москвы

import pandas as pd
from sqlalchemy import create_engine
import h3
from shapely.geometry import Point
import geopandas as gpd
from enums import postgress_connection, postgress_db, lp, schema, RES

engine = create_engine(f'postgresql://{lp}@{postgress_connection}/{postgress_db}')
    
df = pd.read_csv('../data/msk_population_data.csv')
print('count rows = ',len(df))

df['geometry']  = df[['lat','lon']].apply(lambda x: Point(x['lon'],x['lat']),axis = 1)
df['geo_h3_10'] = df[['lat','lon']].apply(lambda x: h3.geo_to_h3(x['lat'],x['lon'],RES),axis=1)
gdf = gpd.GeoDataFrame(df)
gdf = gdf.set_crs(4326)
gdf.to_postgis(schema=schema,name = 'popul_msk_raw_all',\
                  con = engine,if_exists = 'replace')
print("table popul_msk_raw_all created")

