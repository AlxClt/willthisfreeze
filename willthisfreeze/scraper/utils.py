import json
import math
import requests
from typing import List
from pyproj import Transformer

import importlib_resources


def to_latlon(x, y, source_epsg=3857):
    """
    Convert projected coordinates (x, y) to latitude and longitude.

    Parameters:
        x, y: coordinates in meters
        source_epsg: EPSG code of the input projection (default UTM zone 33N)

    Returns:
        (latitude, longitude) in decimal degrees
    """
    transformer = Transformer.from_crs(f"EPSG:{source_epsg}", "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(x, y)
    return lat, lon

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
    if coords:
        lat, lon = to_latlon(*coords)
    else:
        lat, lon = None, None

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


def get_title(route: dict) -> str:
    
    locales = route.get("locales", {}) or {}
    locales = list(filter(filter_langs,  locales)) # filter_langs does the right job in this case: keep only the french version

    waypoint = route.get("associations", {}).get("waypoints", [{}])[0].get("locales", [])  #first waypoint should be the main (not always true?)
    waypoint = list(filter(filter_langs,  waypoint))
    
    if (len(waypoint)==0) & (len(locales)==0):
        return ''

    if len(locales)==0:
        return waypoint[0].get("title", "")
    
    if len(waypoint)==0:
        return locales[0].get("title", "")
    
    return ', '.join([waypoint[0].get("title", ""), locales[0].get("title", "")])


def read_config() -> dict:
    
    my_resources = importlib_resources.files("willthisfreeze")
    data = json.loads(my_resources.joinpath("scraper", "config.json").read_bytes())

    return data

def read_secret() -> dict:
    
    my_resources = importlib_resources.files("willthisfreeze")
    data = json.loads(my_resources.joinpath("scraper", "secret.json").read_bytes())

    return data


#--------------------------
# Utilities for weather stations attribution
#--------------------------

def bounding_box(coord, max_distance_km):
    """
    Calculate a bounding box around a coordinate such that all points
    within the box are within `max_distance_km` from the coordinate.

    Parameters:
        coord: tuple (lat, lon) in decimal degrees
        max_distance_km: maximum distance in kilometers

    Returns:
        (min_lat, max_lat, min_lon, max_lon)
    """
    lat, lon = coord
    R = 6371.0  # Earth radius in km

    # Latitude bounds (1 degree ≈ 111 km)
    delta_lat = (max_distance_km / R) * (180 / math.pi)
    min_lat = lat - delta_lat
    max_lat = lat + delta_lat

    # Longitude bounds depend on latitude
    delta_lon = (max_distance_km / R) * (180 / math.pi) / math.cos(math.radians(lat))
    min_lon = lon - delta_lon
    max_lon = lon + delta_lon

    return (min_lat, max_lat, min_lon, max_lon)

def haversine_distance(coord1, coord2):
    """
    Calculate the great-circle distance between two points on Earth 
    given as (latitude, longitude) pairs using the Haversine formula.
    
    Parameters:
        coord1: tuple of (lat1, lon1) in decimal degrees
        coord2: tuple of (lat2, lon2) in decimal degrees

    Returns:
        Distance in kilometers (float)
    """
    # Radius of Earth in kilometers
    R = 6371.0

    lat1, lon1 = coord1
    lat2, lon2 = coord2

    # Convert decimal degrees to radians
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    # Haversine formula
    a = math.sin(delta_phi / 2.0) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance
