import json
import warnings
import datetime
from functools import partial
from typing import Optional, Set, List, List, Literal
import importlib_resources

from sqlalchemy import Engine, CursorResult
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm.query import Query

from sqlalchemy_utils import database_exists

from willthisfreeze.dbutils.schema import (
    Base, 
    Routes, 
    Orientations, 
    Outings, 
    Countries,
    WeatherStation,
    StationsParameters)

def read_config() -> dict:
    
    my_resources = importlib_resources.files("willthisfreeze")
    data = json.loads(my_resources.joinpath("dbutils", "config.json").read_bytes())

    return data

def get_engine(config: dict) -> Engine:
    dbstring = config['dbstring']
    engine = create_engine(dbstring)
    return engine

def create_local_db() -> None:

    config = read_config()
    dbstring = config['dbstring']

    if database_exists(dbstring):
        warnings.warn('Database already exist. To recreate it, manually delete the db file before calling create_local_db')
    else:
        engine = create_engine(dbstring)
        Base.metadata.create_all(engine, checkfirst=False)
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
get_outings = partial(get_obj, Outings, "outingId")
get_route = partial(get_obj, Routes, "routeId")

def get_orientation(session: Session, orientation: str) -> Orientations:

    orientation_obj = session.scalar(
            select(Orientations).where(Orientations.orientation == orientation)
        )
    if not orientation_obj:
        orientation_obj = Orientations(orientation=orientation)
        session.add(orientation_obj)
    
    return orientation_obj

def insert_route(session: Session,
                 routeId: int,
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
        routeId=routeId,
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
                  outingId: int,
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
        outingId=outingId,
        date=date,
        conditions=conditions,
        last_updated=last_updated,
        routes = routesList
     )
    
    session.add(outing)
    session.commit()

def load_scraped_routes_ids(engine: Engine, min_date: Optional[datetime.datetime]) -> Set[int]:
    """Return set of route IDs updated after min_date (or all if None)."""
    query = "SELECT routeId FROM Routes"
    if min_date:
        query += " WHERE last_updated >= :min_date AND last_updated IS NOT NULL"

    route_ids: Set[int] = set()
    with engine.connect() as conn:
        result = conn.execute(text(query), {"min_date": min_date.strftime("%Y-%m-%d")} if min_date else {})
        route_ids = {row.routeId for row in result}

    return route_ids

def load_routes(session: Session, countryId: Optional[int] = None) -> Query:
    """
    Returns ORM objects for Routes filtered by country Id if provided
    call .all() on the result to retrieve it if you don't need to query it further
    """
    if not countryId:
        return session.query(Routes)
    
    return (
        session.query(Routes)
        .join(Routes.countries)
        .filter(Countries.countryId == countryId)
    )

def load_scraped_outings_ids(engine: Engine, min_date: Optional[datetime.datetime], mode: Literal['update_date', 'outing_date']) -> Set[int]:
    """Return set of route IDs updated after min_date, or outings that happened after min_date"""

    if mode not in {'update_date', 'outing_date'}:
        raise ValueError("mode must be either 'update_date' or 'outing_date'")
        
    query = "SELECT outingId FROM Outings"
    if min_date: 
        if mode=='update_date':
            query += " WHERE last_updated >= :min_date AND last_updated IS NOT NULL"
        else:
            query += " WHERE date >= :min_date"

    outings_ids: Set[int] = set()
    with engine.connect() as conn:
        result = conn.execute(text(query), {"min_date": min_date.strftime("%Y-%m-%d")} if min_date else {})
        outings_ids = {row.outingId for row in result}

    return outings_ids

def get_last_outing_date(engine: Engine) -> datetime.datetime:
    """Return the date of the most recent outing in db"""
    query = "SELECT MAX(date) FROM Outings"

    with engine.connect() as conn:
        result_str = conn.execute(text(query)).scalar_one()
        result = datetime.datetime.strptime(result_str, "%Y-%m-%d")
        
    return result

def check_route_existence(engine: Engine, routeId: int) -> bool:
    """Checks whether the route exists in db"""
    query = "SELECT routeId FROM Routes WHERE routeId == :routeId"

    with engine.connect() as conn:
        route = conn.execute(text(query), {"routeId": routeId})

    result = route.first() is not None

    return result

def check_outing_existence(engine: Engine, outingId: int) -> bool:

    """Checks whether the outing exists in db"""
    query = "SELECT outingId FROM Outings WHERE outingId == :outingId"

    outings_ids: Set[int] = set()
    with engine.connect() as conn:
        result = conn.execute(text(query), {"outingId": outingId})
        outings_ids = {row.outingId for row in result}

    result = (len(outings_ids)>0)
    return result

# -----------------------
# Weather data
# -----------------------

get_weather_station_parameter = partial(get_obj, StationsParameters, "parameterName")
get_weather_station = partial(get_obj, WeatherStation, "stationId")

def insert_weather_station(session: Session,
                           stationId: str,
                           name: str,
                           dateStart:datetime.datetime,
                           dateEnd:datetime.datetime,
                           altitude: int,
                           lat: float,
                           lon: float,
                           lastUpdated:datetime.datetime,
                           ofInterest: bool = True,
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
        stationId=stationId,
        name=name,
        dateStart=dateStart,
        dateEnd=dateEnd,
        altitude=altitude,
        lat=lat,
        lon=lon,
        lastUpdated=lastUpdated,
        ofInterest=ofInterest,
        parameters = stationParamsList,
        routes = routesList
     )
    
    session.add(station)
    if commit:
        session.commit()

def insert_weather_station_parameter(session: Session,
                                     parameterName: str,
                                     lastUpdated:datetime.datetime,
                                     parameterId: int | None = None,
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
        parameterName=parameterName,
        parameterId=parameterId,
        lastUpdated=lastUpdated,
        stations = stationsList
     )
    
    session.add(param)
    session.commit()

def load_scraped_stations_ids(engine: Engine) -> Set[str]:
    """Return set of stations IDs already in db."""
    query = "SELECT stationId FROM weather_stations"

    stations_ids: Set[str] = set()
    with engine.connect() as conn:
        result = conn.execute(text(query))
        stations_ids = {str(row.stationId) for row in result}

    return stations_ids

def load_stations(engine: Engine) -> CursorResult[WeatherStation]:
    """Return weather stations in db."""
    query = "SELECT * FROM weather_stations"

    with engine.connect() as conn:
        result = conn.execute(text(query))

    return result

