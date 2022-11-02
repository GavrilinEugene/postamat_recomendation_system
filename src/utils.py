import pickle
import pandas as pd
import numpy as np
import geojson
import json
from h3 import h3


def flatten_json(nested_json, exclude=['']):
    """Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object.
            exclude: Keys to exclude from output.
        Returns:
            The flattened json object if successful, None otherwise.
    """
    out = {}

    def flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude: flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out

def load_pickle(file_path):
    try:
        with open(file_path, 'rb') as handle:
            return pickle.load(handle)
    except Exception as e:
        print(str(e))
        return None
        
def save_pickle(obj, filepath): 
    with open(filepath, 'wb') as handle:
        pickle.dump(obj, handle, protocol=pickle.HIGHEST_PROTOCOL)
                



def make_h3_index(df: pd.DataFrame, lat: str, lon: str, resolution: int):
    """ create h3 index column based on lat, lon and resolution
        : param df: pandas df
        : param lat: latitude column name
        : param lon: longitude column name
        : param resolution: uber h3 resolution
    """
    scales = [resolution] * len(df)
    return list(map(h3.geo_to_h3, df[lat], df[lon], scales))


def geo_lat_lon_from_h3(df: pd.DataFrame, from_h3_column: str, lat: str = 'lat', lon: str= 'lon'):
    """
        Извлечение центра хексагоны по колонке from_h3_column из df pd.Dataframe
    """
    df[lat], df[lon] = zip(*df[from_h3_column].apply(lambda x: h3.h3_to_geo(x)))
    return df


def return_with_check(companies):
    if len(companies) > 0:
        df = pd.concat(companies, axis = 0)
        return df
    return None        


def haversine_np(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    All args must be of equal length.    

    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    angle = np.sin(dlat / 2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0)**2
    km = 6371 * 2 * np.arcsin(np.sqrt(angle))
    return km