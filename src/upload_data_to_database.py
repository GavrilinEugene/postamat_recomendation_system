from sqlalchemy import create_engine
from pathlib import Path
from shapely import wkt
import pandas as pd
import geopandas as gpd
import h3pandas
from transliterate import translit
import re
import utils as gt


from enums import postgress_connection, postgress_db, lp, schema, RES
engine = create_engine(f'postgresql://{lp}@{postgress_connection}/{postgress_db}')


BASE_PATH = Path(__file__).parent.parent
DATA_PATH = BASE_PATH / 'data'



def upload_isochrones():
    df_isochrones = pd.read_csv(DATA_PATH / "isochrones_walking.csv", sep = ';')
    df_isochrones = df_isochrones[df_isochrones['kind'] != 'walking_20min']
    df_isochrones['geometry'] = df_isochrones['geometry'].apply(wkt.loads)
    gdf_isochrones = gpd.GeoDataFrame(df_isochrones, geometry = 'geometry', crs = 'epsg:4326')
    gdf_isochrones.to_postgis("platform_isochrones", engine, schema = schema, if_exists='replace', index=None)


def upload_domain():
    df_domain = pd.read_csv(DATA_PATH / "moscow_domain.csv", sep = ';')
    df_domain = df_domain[df_domain['okrug_name']!= 'Зеленоградский административный округ']
    df_domain['geometry'] = df_domain['geometry'].apply(wkt.loads)
    gdf_domain = gpd.GeoDataFrame(df_domain, geometry = 'geometry', crs = 'epsg:4326')
    gdf_domain.to_postgis("platform_domain", engine, schema = schema, if_exists='replace', index=None)


def upload_h3_dict():
    df_isochrones = pd.read_csv(DATA_PATH / "isochrones_walking.csv", sep = ';')
    df_isochrones = df_isochrones[df_isochrones['kind'] != 'walking_20min']
    df_space = df_isochrones[['geo_h3_10']].drop_duplicates()
    df_space = gt.geo_lat_lon_from_h3(df_space, f'geo_h3_{RES}')
    df_space['point'] = gpd.points_from_xy(df_space['lon'], df_space['lat'])
    gdf_space = gpd.GeoDataFrame(df_space, geometry = 'point', crs = 'epsg:4326')
    gdf_space.to_postgis("h3_to_latlon", engine, schema = 'postamat', if_exists='replace', index=None)


def upload_rubrics():
    df_copmanies = pd.read_csv(DATA_PATH / "popular_rubrics.csv", sep = ';')
    df_copmanies['geometry'] = gpd.points_from_xy(df_copmanies['point_lon'], df_copmanies['point_lat'])
    gdf_companies = gpd.GeoDataFrame(df_copmanies, geometry = 'geometry', crs = 'epsg:4326')
    gdf_companies.to_postgis("platform_companies", engine, schema = schema, if_exists='replace', index=None)


def upload_features():

    df_population = pd.read_csv(DATA_PATH / "population.csv", sep = ';')
    df_population.rename(columns = {f'geo_h3_{RES}': 'h3_polyfill'}, inplace=True)

    df_copmanies = pd.read_csv(DATA_PATH / "popular_rubrics.csv", sep = ';')
    df_copmanies.rename(columns = {f'geo_h3_{RES}': 'h3_polyfill'}, inplace=True)
    good_rubrics = df_copmanies['rubric'].value_counts().head(100).index
    df_copmanies = df_copmanies[df_copmanies['rubric'].isin(good_rubrics)]
    df_copmanies['point'] = gpd.points_from_xy(df_copmanies['point_lon'], df_copmanies['point_lat'])
    df_copmanies = df_copmanies.drop(['point_lat', 'point_lon'], axis = 1)
    df_copmanies_gr = df_copmanies.groupby(['h3_polyfill', 'rubric'])['id'].nunique().reset_index()
    df_copmanies_gr = pd.pivot_table(
        df_copmanies_gr, index='h3_polyfill', columns='rubric', values='id', aggfunc='sum')

    df_isochrones = pd.read_csv(DATA_PATH / "isochrones_walking.csv", sep = ';')
    df_isochrones = df_isochrones[df_isochrones['kind'] != 'walking_20min']
    df_isochrones['geometry'] = df_isochrones['geometry'].apply(wkt.loads)


    gdf_shape = gpd.GeoDataFrame(df_isochrones, geometry = 'geometry')
    gdf_shape = gdf_shape.h3.polyfill_resample(RES).reset_index()
    df_features = gdf_shape[[f'geo_h3_{RES}', 'h3_polyfill', 'kind']].copy()
    df_features = pd.merge(df_features, df_population, on = ['h3_polyfill'], how = 'left')
    df_features = pd.merge(df_features, df_copmanies_gr, on = ['h3_polyfill'], how = 'left')

    feat_columns = df_features.columns[3:]
    l = []
    for kind in df_features['kind'].unique():
        df_features_kind = df_features[df_features['kind'] == kind].groupby(
            [f'geo_h3_{RES}', 'kind'])[feat_columns].sum().reset_index()
        l.append(df_features_kind)
        
    df_features_gr = pd.concat(l, axis = 0)
    new_columns = [re.sub(r'[^\w\s]', '', translit(x, "ru", reversed=True)).strip().replace(' ', '_') for x in df_features_gr.columns]
    df_features_gr.columns = new_columns
    df_features_gr.columns = [x.lower() for x in df_features_gr.columns]    

    df_features_gr.to_sql("platform_features", engine, schema = 'postamat', if_exists='replace', index=None)


def upload_buildings():

    df_features_gr = pd.read_csv(DATA_PATH / "houses.csv", sep = ';')
    df_features_gr['geometry'] = df_features_gr['geometry'].apply(wkt.loads)
    df_features_gr = gpd.GeoDataFrame(df_features_gr, geometry = 'geometry', crs = 'epsg:4326')
    df_features_gr.to_postgis("platform_buildings", engine, schema = 'postamat', if_exists='replace', index=None)


# upload_features()

# upload_isochrones()

# upload_domain()

upload_rubrics()





