import warnings
import datetime
from functools import partial
from typing import Optional, Set, List, List, Literal

from sqlalchemy import Engine, CursorResult
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm.query import Query

from sqlalchemy_utils import database_exists

from willthisfreeze.config import read_config
from willthisfreeze.dbutils.schema import (
    Base, 
    Routes, 
    Orientations, 
    Outings, 
    Countries,
    WeatherStation,
    StationsParameters)

def get_engine(dbstring: str) -> Engine:
    engine = create_engine(dbstring)
    return engine

def create_db(dbstring) -> None:

    engine = create_engine(dbstring)
    Base.metadata.create_all(engine, checkfirst=True)
    populate_orientation_table(engine)

def get_obj(Obj, idColumn: str, session: Session, objData: dict):

    obj = session.scalar(
        select(Obj).where(getattr(Obj, idColumn) == objData[idColumn])
    )
    if not obj:
        obj = Obj(**objData)
        session.add(obj)

    return obj

# -----------------------
# Routes data
# -----------------------

def populate_orientation_table(engine) -> None:

    with Session(engine) as session:

        conf = read_config()
        
        for orient in conf['orientations']:
            session.add(Orientations(orientation=orient))
        
        session.commit()
    
    return

get_country = partial(get_obj, Countries, "countryName")
get_outings = partial(get_obj, Outings, "outing_id")
get_route = partial(get_obj, Routes, "route_id")

def get_orientation(session: Session, orientation: str) -> Orientations:

    orientation_obj = session.scalar(
            select(Orientations).where(Orientations.orientation == orientation)
        )
    if not orientation_obj:
        orientation_obj = Orientations(orientation=orientation)
        session.add(orientation_obj)
    
    return orientation_obj

def insert_route(session: Session,
                 route_id: int,
                 name: str,
                 lat: float | None = None,
                 lon: float | None = None,
                 snow_ice_mixed: int | None = None,
                 mountain_climbing: int | None = None,
                 ice_climbing: int | None = None,
                 elevation_min: int | None = None,
                 elevation_max: int | None = None,
                 difficulties_height: int | None = None,
                 height_diff_difficulties: int | None = None,
                 glacier: str | None = None,
                 global_rating: str | None = None,
                 ice_rating: str | None = None,
                 mixed_rating: str | None = None,
                 rock_free_rating: str | None = None, 
                 orientations: List = [],
                 outings: List = [],
                 countries: List =  [],
                 weather_stations: List =  [],
                 last_updated: str | None = None
                 ) -> None:

    # Retrieving orientations
    orientationsList: List[Orientations] = []
    for orient in orientations:
        orientationsList.append(get_orientation(session, orient))

    
    # Retrieving countries
    countriesList: List[Countries] = []
    for countryData in countries:
        countriesList.append(get_country(session, countryData))

    # Retrieving outings
    outingsList: List[Outings] = []
    for outingData in outings:
        outingsList.append(get_outings(session, outingData))

    # Retrieving stations
    stationsList: List[WeatherStation] = []
    for stationData in weather_stations:
        stationsList.append(get_weather_station(session, stationData))

    route = Routes(
        route_id=route_id,
        name=name,
        lat=lat,
        lon=lon,
        snow_ice_mixed=snow_ice_mixed,
        mountain_climbing=mountain_climbing,
        ice_climbing=ice_climbing,
        elevation_min=elevation_min,
        elevation_max=elevation_max,
        difficulties_height=difficulties_height,
        height_diff_difficulties=height_diff_difficulties,
        glacier=glacier,
        global_rating=global_rating,
        ice_rating=ice_rating,
        mixed_rating=mixed_rating,
        rock_free_rating=rock_free_rating, 
        orientations=orientationsList,
        outings=outingsList,
        countries = countriesList,
        stations = stationsList,
        last_updated = last_updated
     )
    
    session.add(route)
    session.commit()

def insert_outing(session: Session,
                  outing_id: int,
                  date: str,
                  conditions: str | None,
                  last_updated: str | None = None,
                  routes: List = []
                 ) -> None:

    """
    Mainly used to add an outing to a previously parsed route
    """

    # Retrieving routes 
    routesList: List[Routes] = []
    for routeData in routes:
        routesList.append(get_route(session, routeData))

    outing = Outings(
        outing_id=outing_id,
        date=date,
        conditions=conditions,
        last_updated=last_updated,
        routes = routesList
     )
    
    session.add(outing)
    session.commit()

def load_scraped_routes_ids(engine: Engine, min_date: Optional[datetime.datetime]) -> Set[int]:
    """Return set of route IDs updated after min_date (or all if None)."""
    query = "SELECT route_id FROM Routes"
    if min_date:
        query += " WHERE last_updated >= :min_date AND last_updated IS NOT NULL"

    route_ids: Set[int] = set()
    with engine.connect() as conn:
        result = conn.execute(text(query), {"min_date": min_date.strftime("%Y-%m-%d")} if min_date else {})
        route_ids = {row.route_id for row in result}

    return route_ids

def load_routes(session: Session, country_id: Optional[int] = None) -> Query:
    """
    Returns ORM objects for Routes filtered by country Id if provided
    call .all() on the result to retrieve it if you don't need to query it further
    """
    if not country_id:
        return session.query(Routes)
    
    return (
        session.query(Routes)
        .join(Routes.countries)
        .filter(Countries.country_id == country_id)
    )

def load_scraped_outings_ids(engine: Engine, min_date: Optional[datetime.datetime], mode: Literal['update_date', 'outing_date']) -> Set[int]:
    """Return set of route IDs updated after min_date, or outings that happened after min_date"""

    if mode not in {'update_date', 'outing_date'}:
        raise ValueError("mode must be either 'update_date' or 'outing_date'")
        
    query = "SELECT outing_id FROM Outings"
    if min_date: 
        if mode=='update_date':
            query += " WHERE last_updated >= :min_date AND last_updated IS NOT NULL"
        else:
            query += " WHERE date >= :min_date"

    outings_ids: Set[int] = set()
    with engine.connect() as conn:
        result = conn.execute(text(query), {"min_date": min_date.strftime("%Y-%m-%d")} if min_date else {})
        outings_ids = {row.outing_id for row in result}

    return outings_ids

def get_last_outing_date(engine: Engine) -> datetime.datetime:
    """Return the date of the most recent outing in db"""
    query = "SELECT MAX(date) FROM Outings"

    with engine.connect() as conn:
        result_str = conn.execute(text(query)).scalar_one()
        result = datetime.datetime.strptime(result_str, "%Y-%m-%d")
        
    return result

def check_route_existence(engine: Engine, route_id: int) -> bool:
    """Checks whether the route exists in db"""
    query = "SELECT route_id FROM Routes WHERE route_id == :route_id"

    with engine.connect() as conn:
        route = conn.execute(text(query), {"route_id": route_id})

    result = route.first() is not None

    return result

def check_outing_existence(engine: Engine, outing_id: int) -> bool:

    """Checks whether the outing exists in db"""
    query = "SELECT outing_id FROM Outings WHERE outing_id == :outing_id"

    outings_ids: Set[int] = set()
    with engine.connect() as conn:
        result = conn.execute(text(query), {"outing_id": outing_id})
        outings_ids = {row.outing_id for row in result}

    result = (len(outings_ids)>0)
    return result

# -----------------------
# Weather data
# -----------------------

get_weather_station_parameter = partial(get_obj, StationsParameters, "parameter_name")
get_weather_station = partial(get_obj, WeatherStation, "station_id")

def insert_weather_station(session: Session,
                           station_id: str,
                           name: str,
                           date_start:datetime.datetime,
                           date_end:datetime.datetime,
                           altitude: int,
                           lat: float,
                           lon: float,
                           last_updated:datetime.datetime,
                           of_interest: bool = True,
                           station_parameters: List = [],
                           routes: List = [],
                           commit: bool = True) -> None:

    """
    Adding a weather station
    """

    # Retrieving parameters 
    stationParamsList: List[StationsParameters] = []
    for paramData in station_parameters:
        stationParamsList.append(get_weather_station_parameter(session, paramData))

    # Retrieving associated routes 
    routesList: List[Routes] = []
    for routeData in routes:
        stationParamsList.append(get_route(session, routeData))

    station = WeatherStation(
        station_id=station_id,
        name=name,
        date_start=date_start,
        date_end=date_end,
        altitude=altitude,
        lat=lat,
        lon=lon,
        last_updated=last_updated,
        of_interest=of_interest,
        parameters = stationParamsList,
        routes = routesList
     )
    
    session.add(station)
    if commit:
        session.commit()

def insert_weather_station_parameter(session: Session,
                                     parameter_name: str,
                                     last_updated:datetime.datetime,
                                     parameter_id: int | None = None,
                                     stations: List = []
                                     ) -> None:

    """
    Adding a weather station parameter
    """

    # Retrieving stations 
    stationsList: List[WeatherStation] = []
    for stationData in stations:
        stationsList.append(get_weather_station(session, stationData))

    param = StationsParameters(
        parameter_name=parameter_name,
        parameter_id=parameter_id,
        last_updated=last_updated,
        stations = stationsList
     )
    
    session.add(param)
    session.commit()

def load_scraped_stations_ids(engine: Engine) -> Set[str]:
    """Return set of stations IDs already in db."""
    query = "SELECT station_id FROM weather_stations"

    stations_ids: Set[str] = set()
    with engine.connect() as conn:
        result = conn.execute(text(query))
        stations_ids = {str(row.station_id) for row in result}

    return stations_ids

def load_stations(engine: Engine) -> CursorResult[WeatherStation]:
    """Return weather stations in db."""
    query = "SELECT * FROM weather_stations"

    with engine.connect() as conn:
        result = conn.execute(text(query))

    return result

