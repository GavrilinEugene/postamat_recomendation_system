#Загрузка данных по населению Москвы

import pandas as pd
from sqlalchemy import create_engine
import h3
from shapely.geometry import Point
import geopandas as gpd
import argparse



def run(args):
    
    login = args.login
    password = args.password
    host = args.serv
    port  = args.port
    
    engine = create_engine(f'postgresql://{login}:{password}@{host}:{port}/geo')
    
    df = pd.read_csv('../data/msk_population_data.csv')
    print('count rows = ',len(df))

    df['geometry']  = df[['lat','lon']].apply(lambda x: Point(x['lon'],x['lat']),axis = 1)
    df['geo_h3_10'] = df[['lat','lon']].apply(lambda x: h3.geo_to_h3(x['lat'],x['lon'],10),axis=1)
    gdf = gpd.GeoDataFrame(df)
    gdf = gdf.set_crs(4326)
    gdf.to_postgis(schema='postamat',name = 'popul_msk_raw_all',\
                  con = engine,if_exists = 'replace')
    print("table popul_msk_raw_all created")



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='desc')
    parser.add_argument('-l', '--login', default='')
    parser.add_argument('-p', '--password', default='')
    parser.add_argument('-s', '--serv', default='')
    parser.add_argument('-port', '--port', default='')
    args = parser.parse_args()
    run(args)  