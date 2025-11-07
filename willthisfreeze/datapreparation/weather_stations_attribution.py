from typing import List
from sqlalchemy import CursorResult, Engine, text

from willthisfreeze.datapreparation.utils import bounding_box
from willthisfreeze.dbutils import load_routes, load_stations
from willthisfreeze.dbutils.schema import Routes, WeatherStation

def load_stations_within_radius(engine: Engine, route_lon: float, route_lat: float, radius: float) -> CursorResult[WeatherStation]:
    """Return weather stations in db within a radius of radius km from the given route coordinates"""
    query = "SELECT * FROM weather_stations WHERE lat BETWEEN :min_lat AND :max_lat AND lon BETWEEN :min_lon AND :max_lon"

    min_lat, max_lat, min_lon, max_lon = bounding_box(coord=(route_lat, route_lon), max_distance_km=radius)

    with engine.connect() as conn:
        result = conn.execute(text(query), {"min_lat": min_lat, "min_lon": min_lon, "max_lat": max_lat, "max_lon": max_lon})
    return result

def filter_stations(route:Routes, stations: List[WeatherStation]) -> List[WeatherStation]:
    # computes the distance and keeps the top 10
    if len(stations)<=10:
        return stations
    else:
        res = list(filter())

