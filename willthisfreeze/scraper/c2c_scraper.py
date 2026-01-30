import logging
import datetime
import requests
import multiprocessing
import time
from sys import stdout
import math
from functools import partial
from typing import Literal, Optional, Set, Dict, List, Any

from tqdm import tqdm
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine, Engine

from willthisfreeze.dbutils import (
    insert_route,
    insert_outing,
    load_scraped_outings_ids,
    load_scraped_routes_ids,
    check_route_existence,
    get_last_outing_date,
)

from willthisfreeze.scraper.utils import (
    get_countries_list,
    get_geo_coordinates,
    get_title,
)

disable_tqdm = not stdout.isatty()
logger = logging.getLogger(__name__)

# An iterable that loops over the pages of an api call, be it routes or outings so that it can be iterated over
class C2CApiCallIterator:
    def __init__(
        self,
        api_call_adress: str,
        results_per_page: int = 100,
        timeout_s: float = 20.0,
        max_retries: int = 3,
        backoff_s: float = 1.0,
    ) -> None:
        self.results_per_page = results_per_page
        self.api_call_adress = api_call_adress
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.backoff_s = backoff_s

        self.num_iter_loops = 1
        self.current_loop = 1
        assert 0 < self.results_per_page <= 100

        logger.info("c2c.api.iterator.start", extra={"url": self.api_call_adress, "limit": self.results_per_page})

    def _update_num_iter_loops(self, callresult: dict) -> None:
        assert self.current_loop == 1
        total = callresult.get("total", 0)
        self.num_iter_loops = math.ceil(total / self.results_per_page) if total else 1
        logger.info(
            "c2c.api.iterator.pages",
            extra={"url": self.api_call_adress, "total": total, "limit": self.results_per_page, "total_pages": self.num_iter_loops},
        )

    def __iter__(self):
        return self

    def _get_with_retries(self, params: dict, page: int) -> requests.Response:
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            t0 = time.time()
            try:
                resp = requests.get(self.api_call_adress, params=params, timeout=self.timeout_s)
                dt_ms = int((time.time() - t0) * 1000)

                if resp.status_code == 200:
                    logger.debug("c2c.api.page", extra={**params, "url": self.api_call_adress, "page": page, "status_code": 200, "attempt": attempt, "duration_ms": dt_ms})
                    return resp

                # Retry on 429 / 5xx
                if resp.status_code == 429 or 500 <= resp.status_code < 600:
                    logger.warning(
                        "c2c.api.retryable_status",
                        extra={**params, "url": self.api_call_adress, "page": page, "status_code": resp.status_code, "attempt": attempt, "duration_ms": dt_ms},
                    )
                else:
                    # Non-retryable
                    snippet = (resp.text or "")[:300]
                    logger.error(
                        "c2c.api.failed",
                        extra={**params, "url": self.api_call_adress, "page": page, "status_code": resp.status_code, "attempt": attempt, "duration_ms": dt_ms, "body_snippet": snippet},
                    )
                    resp.raise_for_status()

            except Exception as e:
                last_exc = e
                dt_ms = int((time.time() - t0) * 1000)
                logger.warning(
                    "c2c.api.exception",
                    extra={**params, "url": self.api_call_adress, "page": page, "attempt": attempt, "duration_ms": dt_ms, "error": str(e)},
                )

            # backoff before next attempt
            time.sleep(self.backoff_s * (2 ** (attempt - 1)))

        assert last_exc is not None
        logger.error("c2c.api.give_up", extra={**params, "url": self.api_call_adress, "page": page, "retries": self.max_retries})
        raise last_exc

    def __next__(self):
        if self.current_loop > self.num_iter_loops:
            logger.info("c2c.api.iterator.done", extra={"url": self.api_call_adress, "pages": self.num_iter_loops})
            raise StopIteration

        offset = (self.current_loop - 1) * self.results_per_page
        params = {"offset": offset, "limit": self.results_per_page}

        resp = self._get_with_retries(params=params, page=self.current_loop)
        callresult = resp.json()

        if self.current_loop == 1:
            self._update_num_iter_loops(callresult)

        self.current_loop += 1
        return callresult



class C2CScraper:
    def __init__(
        self,
        config: dict,
        dbstring: str,
        mode: Optional[Literal["init", "update"]] = None,
        update_date_start: Optional[datetime.datetime] = None,
    ) -> None:
        self.mode = mode or "update"
        if self.mode not in {"init", "update"}:
            raise ValueError("mode must be either 'init' or 'update'")

        self.config = config
        self.update_date = datetime.datetime.now().strftime("%Y-%m-%d")

        self.dbstring: str = dbstring
        self.parallel: bool = self.mode in config["parallel"]
        self.num_processes: int = config.get("num_processes", 1)

        self.debug_mode: bool = config.get("debug_mode", False)
        self.scraping_params: Dict[str, Any] = config.get("c2c_scraper_parameters", {})
        self.update_date_start = update_date_start

        logger.info(
            "c2c.init",
            extra={
                "mode": self.mode,
                "parallel": self.parallel,
                "num_processes": self.num_processes,
                "debug_mode": self.debug_mode,
            },
        )

        if self.debug_mode:
            logger.warning("c2c.debug_mode_enabled")

    # -----------------------
    # Scraping (workers)
    # -----------------------
    @staticmethod
    def scrape_route(
        routeData: Optional[dict] = None,
        route_id: Optional[int] = None,
        routes_url: str = "",
        outings_url: str = "",
        routes_filter: str = "",
        already_scraped_ids: Optional[Set[int]] = None,
        update_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
        force_api_call: bool = False,
        get_full_title: bool = True,
        request_timeout_s: float = 30.0,
    ) -> dict:
        """
        Scrape a single route.
        Returns dict safe for parallel usage.
        Prefer returning error info rather than logging heavily inside workers.
        """
        already_scraped_ids = already_scraped_ids or set()
        t0 = time.time()

        try:
            if route_id and routeData is None:
                r = requests.get(f"{routes_url}/{route_id}", timeout=request_timeout_s)
                r.raise_for_status()
                routeData = r.json()

            if not isinstance(routeData, dict) or not routeData:
                raise ValueError("Must provide either route_id or routeData")

            route_id = routeData.get("document_id", route_id)
            if not route_id:
                raise ValueError("Route ID could not be determined")

            if force_api_call:
                r = requests.get(f"{routes_url}/{route_id}", timeout=request_timeout_s)
                r.raise_for_status()
                routeData = r.json()
                if not isinstance(routeData, dict) or not routeData:
                    raise ValueError(f"Couldn't scrape route data for route_id={route_id}")

            if route_id in already_scraped_ids:
                return {"route_id": route_id, "skipped": True, "routeInfo": {}}

            call_address = f"{outings_url}?{routes_filter}{route_id}"
            outingsIterator = C2CApiCallIterator(api_call_adress=call_address, results_per_page=100)

            associated_outings = [
                {
                    "outing_id": outing["document_id"],
                    "date": outing["date_start"],
                    "conditions": outing.get("condition_rating", None),
                    "last_updated": update_date,
                }
                for outinglist in outingsIterator
                for outing in outinglist.get("documents", [])
            ]

            if get_full_title:
                r = requests.get(f"{routes_url}/{route_id}", timeout=request_timeout_s)
                r.raise_for_status()
                fullrouteData = r.json()
                if not isinstance(fullrouteData, dict) or not fullrouteData:
                    raise ValueError(f"Couldn't scrape full title data for route_id={route_id}")
                title = get_title(fullrouteData)
            else:
                title = ""

            lon, lat = get_geo_coordinates(routeData)
            countries = get_countries_list(routeData)
            activities = routeData.get("activities", []) or []

            route_info = {
                "route_id": route_id,
                "name": title,
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

            return {
                "route_id": route_id,
                "skipped": False,
                "routeInfo": route_info,
                "duration_ms": int((time.time() - t0) * 1000),
            }

        except Exception as e:
            # Return error info so parent can log once (cleaner + safer with multiprocessing).
            return {
                "route_id": route_id,
                "skipped": False,
                "routeInfo": {},
                "error": str(e),
                "duration_ms": int((time.time() - t0) * 1000),
            }

    @staticmethod
    def scrape_outing(
        outingData: Optional[dict] = None,
        outing_id: Optional[int] = None,
        outings_url: str = "",
        already_scraped_ids: Optional[Set[int]] = None,
        update_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
        force_api_call: bool = True,
        request_timeout_s: float = 30.0,
    ) -> dict:
        already_scraped_ids = already_scraped_ids or set()
        t0 = time.time()

        try:
            if outing_id and outingData is None:
                r = requests.get(f"{outings_url}/{outing_id}", timeout=request_timeout_s)
                r.raise_for_status()
                outingData = r.json()

            if not isinstance(outingData, dict) or not outingData:
                raise ValueError("Must provide either outing_id or outingData")

            outing_id = outingData.get("document_id", outing_id)
            if not outing_id:
                raise ValueError("Outing ID could not be determined")

            if force_api_call:
                r = requests.get(f"{outings_url}/{outing_id}", timeout=request_timeout_s)
                r.raise_for_status()
                outingData = r.json()
                if not isinstance(outingData, dict) or not outingData:
                    raise ValueError(f"Couldn't scrape outing data for outing_id={outing_id}")

            if outing_id in already_scraped_ids:
                return {"outing_id": outing_id, "skipped": True, "outingInfo": {}}

            routes = outingData.get("associations", {}).get("routes", []) or []
            routeList = [{"route_id": r["document_id"]} for r in routes]

            outing_info = {
                "outing_id": outing_id,
                "date": outingData.get("date_start"),
                "conditions": outingData.get("condition_rating"),
                "last_updated": update_date,
                "routes": routeList,
            }

            return {
                "outing_id": outing_id,
                "skipped": False,
                "outingInfo": outing_info,
                "duration_ms": int((time.time() - t0) * 1000),
            }

        except Exception as e:
            return {
                "outing_id": outing_id,
                "skipped": False,
                "outingInfo": {},
                "error": str(e),
                "duration_ms": int((time.time() - t0) * 1000),
            }

    def _scrape_activity(
        self,
        activity: str,
        target: Literal["outings", "routes"],
        scraped_ids: Optional[Set[int]] = None,
    ) -> List[dict]:
        scraped_ids = scraped_ids or set()

        if target == "routes":
            api_url = f"{self.scraping_params['routes_url']}?{self.scraping_params['activities_filter']}{activity}"
            worker_func = partial(
                self.scrape_route,
                routes_url=self.scraping_params["routes_url"],
                outings_url=self.scraping_params["outings_url"],
                routes_filter=self.scraping_params["routes_filter"],
                already_scraped_ids=scraped_ids,
                update_date=self.update_date,
                force_api_call=False,
            )
        elif target == "outings":
            if not self.update_date_start:
                raise ValueError("When scraping outings first, update_date_start must be provided")

            api_url = (
                f"{self.scraping_params['outings_url']}?"
                f"{self.scraping_params['outings_date_filter']}"
                f"{self.update_date_start.strftime('%Y-%m-%d')},{datetime.datetime.now().strftime('%Y-%m-%d')}"
                f"&{self.scraping_params['activities_filter']}{activity}"
            )
            worker_func = partial(
                self.scrape_outing,
                outings_url=self.scraping_params["outings_url"],
                already_scraped_ids=scraped_ids,
                update_date=self.update_date,
                force_api_call=True,
            )
        else:
            raise ValueError("target must be either 'outings' or 'routes'")

        logger.info(
            "c2c.api.start",
            extra={"activity": activity, "target": target, "url": api_url, "parallel": self.parallel},
        )

        callIterator = C2CApiCallIterator(
            api_call_adress=api_url,
            results_per_page=self.scraping_params.get("num_results_per_page", 100),
        )

        final: List[dict] = []
        for batch_i, payload in enumerate(callIterator):
            docs = payload.get("documents", []) or []
            t0 = time.time()

            logger.info(
                "c2c.batch.start",
                extra={"activity": activity, "target": target, "batch": batch_i, "batch_size": len(docs)},
            )

            if self.parallel and len(docs) > 0:
                with multiprocessing.Pool(self.num_processes) as pool:
                    results = pool.map(worker_func, docs)
            else:
                results = [worker_func(doc) for doc in docs]

            # Debug hard-stop
            if self.debug_mode and batch_i > 1:
                logger.warning("c2c.debug_stop", extra={"activity": activity, "target": target, "batch": batch_i})
                final.extend(results)
                break

            # Summarize errors (don’t spam stack traces)
            errors = [r for r in results if r.get("error")]
            if errors:
                logger.warning(
                    "c2c.batch.errors",
                    extra={
                        "activity": activity,
                        "target": target,
                        "batch": batch_i,
                        "error_count": len(errors),
                        "sample_error": errors[0].get("error"),
                    },
                )

            final.extend(results)

            logger.info(
                "c2c.batch.done",
                extra={
                    "activity": activity,
                    "target": target,
                    "batch": batch_i,
                    "batch_size": len(docs),
                    "duration_ms": int((time.time() - t0) * 1000),
                },
            )

        logger.info(
            "c2c.api.done",
            extra={"activity": activity, "target": target, "total_items": len(final)},
        )
        return final

    def _insert_item(self, session: Session, itemdata: Dict) -> None:
        if itemdata.get("route_id"):
            insert_route(session, **itemdata["routeInfo"])
            return

        if itemdata.get("outing_id"):
            outing_id = itemdata["outing_id"]

            # If scrape returned an error, skip insert (and log once)
            if itemdata.get("error"):
                logger.error("c2c.item.error", extra={"target": "outing", "outing_id": outing_id, "error": itemdata["error"]})
                return

            outing_not_written = True
            for route in itemdata["outingInfo"]["routes"]:
                route_id = route["route_id"]
                engine = session.get_bind()
                exists = check_route_existence(engine=engine, route_id=route_id)
                if not exists:
                    logger.info(
                        "c2c.route.missing_for_outing",
                        extra={"route_id": route_id, "outing_id": outing_id},
                    )
                    routeData = self.scrape_route(
                        routeData=None,
                        route_id=route_id,
                        routes_url=self.scraping_params["routes_url"],
                        outings_url=self.scraping_params["outings_url"],
                        routes_filter=self.scraping_params["routes_filter"],
                        already_scraped_ids=None,
                        update_date=self.update_date,
                    )
                    if routeData.get("error"):
                        logger.error(
                            "c2c.item.error",
                            extra={"target": "route", "route_id": route_id, "error": routeData["error"], "outing_id": outing_id},
                        )
                        continue
                    insert_route(session, **routeData["routeInfo"])
                    outing_not_written = False

            if outing_not_written:
                insert_outing(session, **itemdata["outingInfo"])

    def _scrape(self, engine: Engine, target: Literal["outings", "routes"]) -> Dict[str, dict]:
        if target not in {"outings", "routes"}:
            raise ValueError("target must be either 'outings' or 'routes'")

        message: Dict[str, dict] = {}
        for act in self.scraping_params.get("activities_of_interest", []):
            t0 = time.time()
            logger.info("c2c.activity.start", extra={"activity": act, "target": target})

            if target == "routes":
                scraped_ids = load_scraped_routes_ids(engine=engine, min_date=datetime.datetime(2000, 1, 1))
            else:
                scraped_ids = load_scraped_outings_ids(
                    engine=engine,
                    min_date=self.update_date_start - datetime.timedelta(days=30),
                    mode="outing_date",
                )

            logger.info(
                "c2c.db.loaded_ids",
                extra={"activity": act, "target": target, "count": len(scraped_ids)},
            )

            results = self._scrape_activity(activity=act, target=target, scraped_ids=scraped_ids)

            with Session(engine) as session:
                skipped = 0
                written = 0
                errored = 0

                for item in tqdm(results, desc=f"Writing {act}", disable=disable_tqdm):
                    if item.get("skipped"):
                        skipped += 1
                        continue
                    if item.get("error"):
                        errored += 1
                        continue

                    try:
                        self._insert_item(session, item)
                        written += 1
                    except IntegrityError:
                        session.rollback()
                        logger.warning(
                            "c2c.db.integrity_error",
                            extra={
                                "target": target,
                                "route_id": item.get("route_id"),
                                "outing_id": item.get("outing_id"),
                            },
                        )

            message[act] = {
                "total_scraped": len(results),
                "total_written": written,
                "total_skipped": skipped,
                "total_errored": errored,
            }

            logger.info(
                "c2c.activity.done",
                extra={
                    "activity": act,
                    "target": target,
                    "duration_s": round(time.time() - t0, 2),
                    **message[act],
                },
            )

        return message

    def run(self) -> Dict[str, dict]:
        engine = create_engine(self.dbstring)

        logger.info("c2c.run.start", extra={"mode": self.mode})

        if self.mode == "init":
            return self._scrape(engine=engine, target="routes")

        if self.mode == "update":
            if not self.update_date_start:
                self.update_date_start = get_last_outing_date(engine=engine) - datetime.timedelta(days=7)
                logger.info(
                    "c2c.update.window",
                    extra={"update_date_start": self.update_date_start.strftime("%Y-%m-%d")},
                )
            return self._scrape(engine=engine, target="outings")

        raise ValueError(f"Unsupported mode: {self.mode}")
