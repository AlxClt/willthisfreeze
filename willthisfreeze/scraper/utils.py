import json
import math
import requests
from typing import List

import importlib_resources

def filter_area(area: dict) -> bool:
    if area.get('area_type', '')=='country':
        return True
    return False

def filter_langs(lang: dict) -> bool:
    if lang.get('lang', '')=='fr':
        return True
    return False

def get_geo_coordinates(routeData: dict) -> tuple:
    geometry = routeData.get("geometry", {}) or {}
    geom = geometry.get("geom")

    coords = None
    if geom:
        try:
            coords = json.loads(geom).get("coordinates")
        except (json.JSONDecodeError, TypeError):
            coords = None

    lon, lat = coords if coords else (None, None)

    return lon, lat

def get_countries_list(route: dict) -> List:
    
    areas = route.get("areas", {}) or {}
    admin = filter(filter_area,  areas)

    countries = []
    for adm in list(admin):
        lang = list(filter(filter_langs, adm.get('locales', [])))
        name = None
        if len(lang)>0:
            name = lang[0].get('title', None) 
        countries.append({"countryId":adm['document_id'], "countryName":name})
    
    return countries

def read_config() -> dict:
    
    my_resources = importlib_resources.files("willthisfreeze")
    data = json.loads(my_resources.joinpath("scraper", "config.json").read_bytes())

    return data

def read_secret() -> dict:
    
    my_resources = importlib_resources.files("willthisfreeze")
    data = json.loads(my_resources.joinpath("scraper", "secret.json").read_bytes())

    return data


# An iterable that loops over the pages of an api call, be it routes or outings so that it can be iterated over
class C2CApiCallIterator:

    def __init__(self, api_call_adress: str, results_per_page: int = 100) -> None:
        self.results_per_page = results_per_page
        self.api_call_adress = api_call_adress
        self.num_iter_loops = 1
        self.current_loop = 1
        assert 0 < self.results_per_page <= 100  # API limit

    def _update_num_iter_loops(self, callresult: dict) -> None:
        assert self.current_loop == 1
        self.num_iter_loops = math.ceil(callresult["total"] / self.results_per_page)

    def __iter__(self):
        return self
    
    def __next__(self):
        if self.current_loop > self.num_iter_loops:
            raise StopIteration

        resp = requests.get(
            self.api_call_adress,
            params={"offset": (self.current_loop - 1) * self.results_per_page,
                    "limit": self.results_per_page},
            timeout=20
        )
        if resp.status_code != 200:
            raise RuntimeError(f"API failed {resp.status_code}: {resp.text}")
        callresult = resp.json()

        if self.current_loop == 1:
            self._update_num_iter_loops(callresult)

        self.current_loop += 1
        return callresult
