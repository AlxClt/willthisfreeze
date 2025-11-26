import sys
import logging
from tqdm import tqdm
from typing import List
from sqlalchemy.orm import Session

from willthisfreeze.scraper.utils import bounding_box, haversine_distance
from willthisfreeze.dbutils import load_routes, read_config, get_engine, load_routes
from willthisfreeze.dbutils.schema import Routes, WeatherStation


# -----------------------
# This script is a batch job marking the closest weather stations to be used to get weather history for each route
#     pipeline: c2c_scraper -> weather_stations_attribution -> meteofrance_scraper
# -----------------------


# -----------------------
# Logging configuration
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("StationsAttribution")

def load_stations_within_radius(session: Session, lat: float, lon: float, radius: float):
    """Return ORM WeatherStation objects within bounding box."""
    min_lat, max_lat, min_lon, max_lon = bounding_box(coord=(lat, lon), max_distance_km=radius)

    return session.query(WeatherStation).filter(
        WeatherStation.lat.between(min_lat, max_lat),
        WeatherStation.lon.between(min_lon, max_lon),
    ).all()


def filter_stations(route: Routes, stations: List[WeatherStation], nkeep=10) -> List[int]:
    """Compute distance to route and return closest station IDs."""
    return [
        station.stationId
        for station in sorted(
            stations,
            key=lambda s: haversine_distance((route.lat, route.lon), (s.lat, s.lon))
        )[:nkeep]
    ]


def update_routes_station_mapping(session: Session, route: Routes, station_ids: List[int]) -> None:
    stations = session.query(WeatherStation).filter(WeatherStation.stationId.in_(station_ids)).all()
    route.stations = stations
    session.commit()

def update_weather_stations_interest_flag():
    """
    batch update the interest flag marking stations for which weather history should be scraped
    """
    return

def main_weather_stations_attribution():
    conf = read_config()
    engine = get_engine(conf)

    with Session(engine) as session:
        routes = load_routes(session, countryId=14274)

        logger.info("Attributing weather stations to the routes...")
        for route in tqdm(routes):
            stations = load_stations_within_radius(session, route.lat, route.lon, radius=20)
            station_ids = filter_stations(route, stations)
            update_routes_station_mapping(session, route, station_ids)
        logger.info("Weather stations attribution completed")


