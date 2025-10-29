import sys
import logging
import datetime
import requests
import multiprocessing
from functools import partial
from typing import Literal, Optional, Set, Dict, List, Any

from tqdm import tqdm
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text, Engine

from willthisfreeze.dbutils import (
    insert_route, 
    insert_outing,
    load_scraped_outings_ids, 
    load_scraped_routes_ids, 
    check_route_existence, 
    get_last_outing_date
)
from willthisfreeze.scraper.utils import C2CApiCallIterator, get_countries_list, get_geo_coordinates


# -----------------------
# Logging configuration
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("C2CScraper")


class C2CScraper:

    def __init__(self, 
                 config: dict, 
                 mode: Optional[Literal["initial_load", "update"]] = None, 
                 update_date_start: Optional[datetime.datetime] = None
                 ) -> None:
        
        self.mode = mode or "update"

        if self.mode not in {"initial_load", "update"}:
            raise ValueError("mode must be either 'initial_load' or 'update'")

        self.config = config
        self.update_date = datetime.datetime.now().strftime("%Y-%m-%d")

        self.dbstring: str = config["dbstring"]
        self.parallel: bool = self.mode in config['parallel']
        self.num_processes: int = config.get("num_processes", 1)

        self.scraping_params: Dict[str, Any] = config.get("scraping_parameters", {})
        self.update_date_start = update_date_start

    # -----------------------
    # Scraping
    # -----------------------
    @staticmethod
    def scrape_route(
        routeData: Optional[dict] = None,
        routeId: Optional[int] = None,
        routes_url: str = "",
        outings_url: str = "",
        routes_filter: str = "",
        already_scraped_ids: Optional[Set[int]] = None,
        update_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
        force_api_call: bool = False
    ) -> dict:
        """Scrape a single route. Returns dict safe for parallel usage."""
        if already_scraped_ids is None:
            already_scraped_ids = set()

        if routeId:
            routeData = requests.get(f"{routes_url}/{routeId}").json()
            
        if not isinstance(routeData, dict) or not routeData:
            raise ValueError("Must provide either routeId or routeData")

        routeId = routeData.get("document_id", routeId)
        if not routeId:
            raise ValueError("Route ID could not be determined")

        if force_api_call:
            routeData = requests.get(f"{routes_url}/{routeId}").json()
            if not isinstance(routeData, dict) or not routeData:
                raise ValueError("Couldn't scrape route data for route Id %d", routeId)
        
        if routeId in already_scraped_ids:
            return {"routeId": routeId, "skipped": True, "routeInfo": {}}

        call_address = f"{outings_url}?{routes_filter}{routeId}"
        outingsIterator = C2CApiCallIterator(api_call_adress=call_address, results_per_page=100)

        associated_outings = [
            {
                "outingId":outing["document_id"],
                "date":outing["date_start"],
                "conditions":outing.get("condition_rating", None),
                "last_updated":update_date,
            }
            for outinglist in outingsIterator
            for outing in outinglist.get("documents", [])
        ]


        lon, lat = get_geo_coordinates(routeData)
        countries = get_countries_list(routeData)
        activities = routeData.get("activities", []) or [] # sometimes the key exists but the value is None

        route_info = {
            "routeId": routeId,
            "lat": lat,
            "lon": lon,
            "snow_ice_mixed": 1 if "snow_ice_mixed" in activities else None,
            "mountain_climbing": 1 if "mountain_climbing" in activities else None,
            "ice_climbing": 1 if "ice_climbing" in activities else None,
            "elevation_min": routeData.get("elevation_min", None),
            "elevation_max": routeData.get("elevation_max", None),
            "difficulties_height": routeData.get("difficulties_height", None),
            "height_diff_difficulties": routeData.get("height_diff_difficulties", None),
            "orientations": routeData.get("orientations", []) or [],
            "glacier": routeData.get("glacier_gear", None),
            "global_rating": routeData.get("global_rating", None),
            "ice_rating": routeData.get("ice_rating", None),
            "mixed_rating": routeData.get("mixed_rating", None),
            "rock_free_rating": routeData.get("rock_free_rating", None),
            "outings": associated_outings,
            "countries": countries,
            "last_updated": update_date,
        }

        return {"routeId": routeId, "skipped": False, "routeInfo": route_info}


    @staticmethod
    def scrape_outing(
        outingData: Optional[dict] = None,
        outingId: Optional[int] = None,
        outings_url: str = "",
        already_scraped_ids: Optional[Set[int]] = None,
        update_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
        force_api_call: bool = True
    ) -> dict:
        """Scrape a single outing. Returns dict safe for parallel usage."""
        if already_scraped_ids is None:
            already_scraped_ids = set()

        if outingId:
            outingData = requests.get(f"{outings_url}/{outingId}").json()
            
        if not isinstance(outingData, dict) or not outingData:
            raise ValueError("Must provide either outingId or outingData")

        outingId = outingData.get("document_id", outingId)
        if not outingId:
            raise ValueError("Outing ID could not be determined")
        
        # When we get outings not from the direct api call to the outing page, we only get incomplete information
        if force_api_call:
            outingData = requests.get(f"{outings_url}/{outingId}").json()
            if not isinstance(outingData, dict) or not outingData:
                raise ValueError("Couldn't scrape outing data for outing Id %d", outingId)
            
        if outingId in already_scraped_ids:
            return {"outingId": outingId, "skipped": True, "outingInfo": {}}
        
        routeList: List[Dict] = []
        # always at leas one associated route
        routes = outingData.get('associations', {}).get('routes', [])
        routeList = [{'routeId': r["document_id"]} for r in routes]
        logger.debug(outingData)
        outing_info = {
            "outingId": outingId,
            "date": outingData.get("date_start"),
            "conditions": outingData.get("condition_rating"),
            "last_updated": update_date,
            "routes": routeList
        }

        return {"outingId": outingId, "skipped": False, "outingInfo": outing_info}
    

    def _scrape_activity(self, activity: str, target:Literal['outings', 'routes'], scraped_ids: Optional[Set[int]] = None) -> List[dict]:
        """Scrape all routes for a given activity."""
        scraped_ids = scraped_ids or set()

        if target=='routes':
            api_url = f"{self.scraping_params['routes_url']}?{self.scraping_params['activities_filter']}{activity}"
            worker_func = partial(
                self.scrape_route,
                routes_url=self.scraping_params["routes_url"],
                outings_url=self.scraping_params["outings_url"],
                routes_filter=self.scraping_params["routes_filter"],
                already_scraped_ids=scraped_ids,
                update_date=self.update_date,
                force_api_call=False
            )
        elif target=='outings':
            if not self.update_date_start:
                raise ValueError("When scraping outings first, a value must be provided for update_date_start")
            api_url = f"""{self.scraping_params['outings_url']}?{self.scraping_params['outings_date_filter']}{self.update_date_start.strftime("%Y-%m-%d")},{datetime.datetime.now().strftime("%Y-%m-%d")}&{self.scraping_params['activities_filter']}{activity}""" #/outings?date=2025-09-03,2025-09-10&act=snow_ice_mixed
            logger.info("Calling api with url %s", api_url)
            worker_func = partial(
                self.scrape_outing,
                outings_url=self.scraping_params["outings_url"],
                already_scraped_ids=scraped_ids,
                update_date=self.update_date,
                force_api_call=True
            )
        
        else:
            raise ValueError("target must be either 'outings' or 'routes'")

        callIterator = C2CApiCallIterator(
            api_call_adress=api_url,
            results_per_page=self.scraping_params.get("num_results_per_page", 100),
        )

        final: List[dict] = []

        for i, c in enumerate(callIterator):
            documents = c.get("documents", [])
            logger.info("Activity=%s | Batch %d (%d %s)", activity, i, len(documents), target)
            if self.parallel:
                with multiprocessing.Pool(self.num_processes) as pool:
                    results = pool.map(worker_func, documents)
            else:
                results = [worker_func(doc) for doc in documents]

            #if i > 1: # FOR DEBUGGING, REMOVE
            #    logger.info("Hard stopping triggered for debugging")
            #    break
            
            final.extend(results)

        return final

    # -----------------------
    # Workflows
    # -----------------------
    def _insert_item(self, session:Session, itemdata:Dict) -> None:
        if itemdata.get('routeId', None):
            insert_route(session, **itemdata["routeInfo"])
        if itemdata.get('outingId', None):
            outing_not_written = True
            for route in itemdata["outingInfo"]["routes"]: # First we check existence in db of all associated routes. If a route has to be added, the outing will ba automatically adde with it
                routeId = route["routeId"]
                engine = session.get_bind()
                exists = check_route_existence(engine=engine, routeId=routeId)
                if exists:
                    pass
                else:
                    logger.info("Route %d associated with outing %d not found, adding it..", routeId, itemdata["outingId"])
                    routeData = self.scrape_route(
                        routeData = None,
                        routeId = routeId,
                        routes_url=self.scraping_params["routes_url"],
                        outings_url=self.scraping_params["outings_url"],
                        routes_filter=self.scraping_params["routes_filter"],
                        already_scraped_ids=None,
                        update_date=self.update_date,
                    )
                    insert_route(session, **routeData["routeInfo"])
                    outing_not_written = False # outing has been written with route, no need to insert it after

            if outing_not_written:
                insert_outing(session, **itemdata["outingInfo"])


    def _scrape(self, engine: Engine, target: Literal['outings', 'routes']) -> Dict[str, dict]:
        """Perform initial load of all activities."""

        if target not in {'outings', 'routes'}:
            raise ValueError("target must be either 'outings' or 'routes'")
        
        message: Dict[str, dict] = {}

        for act in self.scraping_params.get("activities_of_interest", []):
            logger.info("Starting activity: %s", act)
            # load_scraped_routes cannot get out of the loop because some routes cover sevral activities, so the list of ids must be updated before each iteration
            if target=='routes':
                # All routes in DB
                scraped_ids = load_scraped_routes_ids(
                    engine=engine, min_date=datetime.datetime(2000, 1, 1)
                )
            else:
                # All outings starting after self.update_date_start - 30 days 
                scraped_ids = load_scraped_outings_ids(
                    engine=engine, min_date=self.update_date_start - datetime.timedelta(days=30), mode='outing_date'
                )
            logger.info("Loaded %d scraped %s from DB", len(scraped_ids), target)
            results = self._scrape_activity(activity=act, target=target, scraped_ids=scraped_ids)

            with Session(engine) as session:
                skipped, written = 0, 0
                for item in tqdm(results, desc=f"Writing {act}"):
                    if item["skipped"]:
                        skipped += 1
                    else:
                        self._insert_item(session, item)
                        written += 1

            message[act] = {
                "total_scraped": len(results),
                "total_written": written,
                "total_skipped": skipped,
            }
            logger.info("Activity %s: %s", act, message[act])
        return message


    def run(self) -> Dict[str, dict]:
        """Entry point for scraper."""
        engine = create_engine(self.dbstring)   
        if self.mode == "initial_load":
            # At initial load, the entry point is the route 
            return self._scrape(engine=engine, target='routes')
        elif self.mode == "update":
            if not self.update_date_start:
                # As we only have access to the outing date start and not the date it was uploaded, we check for outings the occured also in the 7 days before the last one in the db
                self.update_date_start = get_last_outing_date(engine=engine) - datetime.timedelta(days=7)
                logger.info("Scraping outings with start dates after %s", self.update_date_start)
            # At update, the entry point is the outing 
            return self._scrape(engine=engine, target='outings')
        else:
            raise ValueError(f"Unsupported mode: {self.mode}")
