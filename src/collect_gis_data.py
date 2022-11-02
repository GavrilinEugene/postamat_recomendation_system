
import pandas as pd
import json
from pathlib import Path
from tqdm import tqdm
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from shapely import wkt

from enums import key, RES
import utils as gt


BASE_PATH = Path(__file__).parent.parent
DATA_PATH = BASE_PATH / 'data'


class GisDataLoader:

    def __init__(self, city, key, cache_date=datetime.today().strftime('%Y%m%d')):
        self.city = city
        self.key = key
        self.date = cache_date
        self.companies_data = self.__load_cache(
            f"{DATA_PATH}/{self.city}_{self.date}")
        print('companies:', len(self.companies_data))
        self.buildings_data = self.__load_cache(f"{DATA_PATH}/{self.city}_buildings")
        print('buildings:', len(self.buildings_data))
        
        
    def get_city_id(self):
        try:
            r = requests.get(f"https://catalog.api.2gis.com/2.0/region/search?q={self.city}&key={self.key}")
            j_dict = json.loads(r.text)
            return int(j_dict['result']['items'][0]['id'])
        except Exception as e:
            return None

    def get_rubric_list(self):
        try:
            id_ = self.get_city_id()
            if id_ is None:
                return None
            r = requests.get(f"https://catalog.api.2gis.com/2.0/catalog/rubric/list?key={self.key}&region_id={id_}&fields=items.rubrics")
            rubric_list = []
            rubrics = json.loads(r.text)
            for idx in range(0, len(rubrics['result']['items'])):
                large_rubric = rubrics['result']['items'][idx]
                for rubric in large_rubric['rubrics']:
                    if 'geo_count' in rubric.keys():
                        cnt = 'geo_count'
                    else:
                        cnt = 'branch_count'
                    rubric_list.append((rubric['name'], rubric[cnt]))
            df_rubrics = pd.DataFrame(rubric_list, columns = ['rubric', 'cnt']).sort_values(by = 'cnt', ascending=False)
            d_rubric_cnt = dict(zip(df_rubrics['rubric'], df_rubrics['cnt']))
        except:
            return None                
        return d_rubric_cnt        

    def __load_cache(self, path):
        data = gt.load_pickle(path)
        if data is None:
            return dict()
        else:
            return data

    def __run_request(self, gis_request):
        r = requests.get(gis_request)
        if r.ok == True:
            soup = BeautifulSoup(r.text, "lxml")
            try:
                content = json.loads(soup.text)
                if content['meta']['code'] == 404:
                    return None, -1
                
                df_res = pd.DataFrame([gt.flatten_json(x)
                                       for x in content['result']['items']])
                total = content['result']['total']
                return df_res, 0, total
            except Exception as e:
                print(str(e))
        return None, -1, -1

    def __load_rubric(self, rubric, url_properties):
        companies = []
        for page in range(1, 250):
            gis_request = f"""https://catalog.api.2gis.com/3.0/items?q={self.city} {rubric}&page={page}&page_size=50&key={self.key}"""+url_properties
            
            data, status = self.__run_request(gis_request)
            if status == -1:
                return gt.return_with_check(companies)
            else:
                companies.append(data)
        return gt.return_with_check(companies)

    def __load_building_info(self, building_id):
        gis_request = f"""https://catalog.api.2gis.com/3.0/items/byid?id={building_id}&fields=items.geometry.hover,items.geometry.centroid,items.structure_info.year_of_construction,items.structure_info.material&key={self.key}"""
        data, _ = self.__run_request(gis_request)
        return data

    def __collect(self, url_properties, dict_cache, save_path):

        city_rubrics = self.get_rubric_list()
        for rubric in tqdm(city_rubrics.keys()):
            # непопулярная рубрика
            if city_rubrics.get(rubric, 0) < 100 or rubric in ['Мусорные контейнеры']:
                continue
    
            # уже скачананя полностью рубрика
            if rubric in dict_cache:
                _, complete = dict_cache[rubric]
                if complete == 1:
                    continue
            try:
                totals = self.__get_rubric_totals(rubric)
                df = self.__load_rubric(rubric, url_properties, totals)
                orig_shape = len(df)
                df['rubric'] = rubric
                df['city'] = self.city
                df['id'] = df['id'].apply(lambda x: x.split('_')[0])
                
                df = df[~df['point_lat'].isna()]
                df = df[['rubric', 'name', 'id',
                     'address_building_id',
                     'rubrics_0_name', 'point_lat', 'point_lon']]
                if df.empty == False:
                    dict_cache[rubric] = (df, int(orig_shape / totals > 0.95))
            except Exception as e:
                print(str(e))
                print(rubric)
                pass

            if len(dict_cache) > 0:
                gt.save_pickle(dict_cache, save_path)
        return dict_cache
    

    def collect_companies(self, rubrics: list):
        
        city_rubrics = self.get_rubric_list()
        if city_rubrics is None:
            intersect_rubrics = rubrics
        else:
            intersect_rubrics = set(rubrics).intersection(city_rubrics)

        url_properties = "&type=branch&fields=items.point,items.adm_div,items.address,items.rubrics"
        path_1 =  self.city + '_' + self.date
        path = str(DATA_PATH / path_1)
        self.companies_data = self.__collect(intersect_rubrics, url_properties,
                                             self.companies_data, path, mode= 'companies')
        
        if len(self.companies_data) > 0:
            data = pd.concat(self.companies_data.values())
            return data
        else:
            return None

    def collect_buildings(self):
        if len(self.companies_data) == 0:
            print("collecting companies first...")
            companies = self.collect_companies()
        else:
            companies = pd.concat(self.companies_data.values())

        buildings_ids = companies['address_building_id'].unique()
        print(f"collecting buildings ({len(buildings_ids)})")

        idx = 0
        for building_id in tqdm(buildings_ids):
            if building_id in self.buildings_data:
                continue
            try:
                data = self.__load_building_info(building_id)
                self.buildings_data[building_id] = data
                idx = idx+1
            except Exception as e:
                continue
        
            if idx % 100 == 0:
                gt.save_pickle(self.buildings_data, f"{DATA_PATH}/{self.city}_buildings") 
        gt.save_pickle(self.buildings_data, f"{DATA_PATH}/{self.city}_buildings")
        return pd.concat(self.buildings_data.values())


dataloader = GisDataLoader('Москва', key, cache_date='20221001')
all_rubrics = dataloader.get_rubric_list()
companies = dataloader.collect_companies(all_rubrics)
buildings = dataloader.collect_buildings()

companies[f'geo_h3_{RES}'] = gt.make_h3_index(companies, 'point_lat', 'point_lon', RES)
companies.to_csv(DATA_PATH / "popular_rubrics.csv", sep = ';', index = None)

buildings['geometry_centroid'] = buildings['geometry_centroid'].apply(wkt.loads)
buildings['lat'] = buildings['geometry_centroid'].apply(lambda x: x.y)
buildings['lon'] = buildings['geometry_centroid'].apply(lambda x: x.x)
buildings[f'geo_h3_{RES}'] = gt.make_h3_index(buildings, 'lat', 'lon', RES)
buildings1 = buildings.drop(['address_name', 'full_name', 'type', 'is_deleted', 'building_name'], axis = 1)
buildings1.rename(columns = {'geometry_hover': 'geometry'}, inplace=True)

df_domain = pd.read_csv(DATA_PATH / "moscow_domain.csv", sep = ';')
buildings1 = buildings1[buildings1[f'geo_h3_{RES}'].isin(df_domain[f'geo_h3_{RES}'])]

buildings1.to_csv(DATA_PATH / "houses.csv", sep = ';', index = None)
