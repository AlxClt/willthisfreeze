import logging
import sys
import os
import time
from typing import List, Literal

from tqdm import tqdm
from sqlalchemy import delete
from sqlalchemy.orm import Session
from sqlalchemy.orm.query import Query

from willthisfreeze.scraper.utils import bounding_box, haversine_distance
from willthisfreeze.dbutils import read_config, get_engine, load_routes
from willthisfreeze.dbutils.schema import Routes, WeatherStation, route_stations_mapping

logger = logging.getLogger(__name__)
disable_tqdm = not sys.stdout.isatty()


# -----------------------
# Helpers
# -----------------------
def reset_attribution(session: Session) -> None:
    logger.warning("ws_attr.reset_mapping")
    session.execute(delete(route_stations_mapping))
    session.commit()


def check_routes_for_update(routes: Query) -> Query:
    logger.info("ws_attr.find_routes_missing_stations")
    return (
        routes.outerjoin(Routes.stations)
        .filter(WeatherStation.station_id.is_(None))
    )


def load_stations_within_radius(session: Session, lat: float, lon: float, radius_km: float):
    """Return ORM WeatherStation objects within bounding box."""
    min_lat, max_lat, min_lon, max_lon = bounding_box(coord=(lat, lon), max_distance_km=radius_km)
    return (
        session.query(WeatherStation)
        .filter(
            WeatherStation.lat.between(min_lat, max_lat),
            WeatherStation.lon.between(min_lon, max_lon),
        )
        .all()
    )


def filter_stations(route: Routes, stations: List[WeatherStation], nkeep: int = 10) -> List[int]:
    """Compute distance to route and return closest station IDs."""
    if not stations:
        return []
    return [
        station.station_id
        for station in sorted(
            stations,
            key=lambda s: haversine_distance((route.lat, route.lon), (s.lat, s.lon)),
        )[:nkeep]
    ]


def update_routes_station_mapping(session: Session, route: Routes, station_ids: List[int]) -> None:
    if not station_ids:
        route.stations = []
        return
    stations = (
        session.query(WeatherStation)
        .filter(WeatherStation.station_id.in_(station_ids))
        .all()
    )
    route.stations = stations


def update_weather_stations_interest_flag(session: Session) -> None:
    """
    Batch update the interest flag marking stations for which weather history should be scraped.
    Since default is true, update stations not existing in the relationship table to False.
    """
    orphan_station_ids = (
        session.query(WeatherStation.station_id)
        .outerjoin(WeatherStation.routes)
        .filter(Routes.route_id.is_(None))
    ).subquery()

    updated = (
        session.query(WeatherStation)
        .filter(WeatherStation.station_id.in_(orphan_station_ids))
        .update({WeatherStation.of_interest: False}, synchronize_session=False)
    )
    session.commit()

    logger.info("ws_attr.station_interest_flag.updated", extra={"stations_set_false": int(updated or 0)})


# -----------------------
# Main batch job
# -----------------------
def weather_stations_attribution(mode: Literal["update", "reset"] = "update") -> None:
    """
    update: add weather stations to routes without stations
    reset: erase all existing links and re-attribute
    """
    t0 = time.time()
    dbstring = os.getenv("DATABASE_URL")
    engine = get_engine(dbstring)

    logger.info("ws_attr.start", extra={"mode": mode, "disable_tqdm": disable_tqdm})

    # Tuneable knobs
    COUNTRY_ID = 14274
    RADIUS_KM = 20
    NKEEP = 10
    COMMIT_EVERY = 200  # reduce transaction overhead

    with Session(engine) as session:
        routes_q = load_routes(session, country_id=COUNTRY_ID)

        if mode == "reset":
            reset_attribution(session)
        else:
            routes_q = check_routes_for_update(routes=routes_q)

        routes = routes_q.all()
        logger.info(
            "ws_attr.routes.loaded",
            extra={"mode": mode, "country_id": COUNTRY_ID, "routes_count": len(routes)},
        )

        processed = 0
        updated = 0
        no_station_candidates = 0
        total_candidates = 0

        logger.info(
            "ws_attr.attribution.begin",
            extra={"radius_km": RADIUS_KM, "nkeep": NKEEP, "commit_every": COMMIT_EVERY},
        )

        for route in tqdm(routes, disable=disable_tqdm):
            processed += 1

            stations = load_stations_within_radius(session, route.lat, route.lon, radius_km=RADIUS_KM)
            total_candidates += len(stations)

            if not stations:
                no_station_candidates += 1
                continue

            station_ids = filter_stations(route, stations, nkeep=NKEEP)
            update_routes_station_mapping(session, route, station_ids)
            updated += 1

            if processed % COMMIT_EVERY == 0:
                session.commit()
                logger.info(
                    "ws_attr.progress",
                    extra={
                        "processed": processed,
                        "updated": updated,
                        "no_station_candidates": no_station_candidates,
                    },
                )

        # final commit for remaining work
        session.commit()

        logger.info(
            "ws_attr.attribution.done",
            extra={
                "processed": processed,
                "updated": updated,
                "no_station_candidates": no_station_candidates,
                "avg_candidates_per_route": (total_candidates / processed) if processed else 0.0,
            },
        )

        logger.info("ws_attr.station_interest_flag.begin")
        update_weather_stations_interest_flag(session=session)

    logger.info("ws_attr.done", extra={"duration_s": round(time.time() - t0, 2)})
