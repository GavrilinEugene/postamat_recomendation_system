import requests
from pathlib import Path
import os
import geopandas as gpd
from tqdm import tqdm
import pandas as pd
import h3pandas
import geojson
from shapely.geometry import Polygon
from enums import token, RES

import utils as gt


BASE_PATH = Path(__file__).parent.parent
DATA_PATH = BASE_PATH / 'data'



def get_isochrone(coord, kind='walking', minutes='5,10,15,20'):
    """

    """
    request = f'https://api.mapbox.com/isochrone/v1/mapbox/{kind}/{coord.x},{coord.y}?contours_minutes={minutes}&polygons=true&access_token={token}'
    r = requests.get(request)
    return geojson.loads(r.content)


def collect_isochrones(df, kind='walking'):
    """
        Функция собирает изохроны по сетке h3, находящися в df Pandas.
        :param df: pd.dataFframe - таблица с h3 индексами, на которые нужно собирать изохроны
        :param kind: типы изохроны
    """
    cache_path = str(DATA_PATH / f"isochrones_walk_{kind}.pkl")
    print(cache_path)
    isochrones_dict = {}
    if os.path.exists(cache_path):
        isochrones_dict = gt.load_pickle(cache_path)
    else:
        print("Файл не найден, считаем с нуля")

    for idx, row in tqdm(df.iterrows(), total=len(df)):
        if row[f'geo_h3_{RES}'] in isochrones_dict.keys():
            if 'features' in isochrones_dict[row[f'geo_h3_{RES}']].keys():
                continue
        isochrones_dict[row[f'geo_h3_{RES}']] = get_isochrone(
            row['point'], kind=kind)
        if idx % 100 == 0:
            gt.save_pickle(isochrones_dict, cache_path)
    gt.save_pickle(isochrones_dict, cache_path)

    l_isochrones = []
    for h3_idx in isochrones_dict.keys():
        l_isochrones.append((h3_idx, f'{kind}_5min', Polygon(isochrones_dict[h3_idx]['features'][3]['geometry']['coordinates'][0])))
        l_isochrones.append((h3_idx, f'{kind}_10min',Polygon(isochrones_dict[h3_idx]['features'][2]['geometry']['coordinates'][0])))
        l_isochrones.append((h3_idx, f'{kind}_15min',Polygon(isochrones_dict[h3_idx]['features'][1]['geometry']['coordinates'][0])))
    df_iso = pd.DataFrame(l_isochrones, columns = [f'geo_h3_{RES}', 'kind', 'geometry'])
    df_iso.to_csv(DATA_PATH / "isochrones_walking.csv", sep = ';', index=None)


def prepare_domin() -> gpd.GeoDataFrame:
    """
        Функция собирает сетку Москвы на h3 index = RES
    """
    # собираем сетку данных, определяем центры квадратов
    df_msk_shape = gpd.read_file(DATA_PATH / "admzones2021" / "admzones2021.shp")
    df_msk_shape = df_msk_shape.drop(['okrug_id', 'area'], axis=1)
    df_msk_shape = df_msk_shape[df_msk_shape['sub_ter'] == 'Старая Москва']
    gdf_shape = gpd.GeoDataFrame(df_msk_shape, geometry = 'geometry')
    gdf_shape = gdf_shape.h3.polyfill_resample(RES).reset_index()
    gdf_shape.rename(columns = {'h3_polyfill': f'geo_h3_{RES}'}, inplace=True)
    gdf_shape = gt.geo_lat_lon_from_h3(gdf_shape, f'geo_h3_{RES}')
    gdf_shape['point'] = gpd.points_from_xy(gdf_shape['lon'], gdf_shape['lat'])
    gdf_shape.read_csv(DATA_PATH / "moscow_domain.csv", sep = ';')
    return gdf_shape


gdf_shape = prepare_domin()
# collect_isochrones(gdf_shape, 'walking')


