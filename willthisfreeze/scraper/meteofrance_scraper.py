import sys
import logging
import requests
from typing import Literal, Optional, Set, Dict, List
import datetime as dt
import time
import os
import sys
import requests
import pandas as pd
from typing import Tuple, List
from tqdm import tqdm
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, Engine


from willthisfreeze.dbutils.dbutils import load_scraped_stations_ids, insert_weather_station


disable_tqdm = not sys.stdout.isatty()
logger = logging.getLogger(__name__)



class MFScraper():

    def __init__(self, 
                 config: dict, 
                 mode: Optional[Literal["init", "update"]] = None):

        self.mode = mode or 'update'
        self.cadence: Literal["horaire","quotidienne","6m","infrahoraire-6m"] = "quotidienne" #restricting this parameter
        self.update_date = dt.datetime.now()

        if self.mode not in {"init", "update"}:
            raise ValueError("mode must be either 'init' or 'update'")

        # base
        self.API_BASE_URL = "https://public-api.meteofrance.fr/public/DPClim/v1"  
        self.API_KEY: str = config["meteoapitoken"]
        self.DBSTRING: str = config["dbstring"]

        self.HEADERS = {
            "Accept": "application/json",
            "apikey": f"{self.API_KEY}"
        }

        self.INFORMATION_STATION = "/information-station"
        self.COMMANDE_STATION_HORAIRE = "/commande-station/horaire"
        self.COMMANDE_STATION_QUOTIDIENNE = "/commande-station/quotidienne"
        self.COMMANDE_STATION_INFRAHORAIRE_6M = "/commande-station/infrahoraire-6m"
        self.COMMANDE_FICHIER = "/commande/fichier"  # download using returned order id
        self.LISTE_STATION_HORAIRE = "/liste-stations/horaire"
        self.LISTE_STATION_QUOTIDIENNE = "/liste-stations/quotidienne"
        self.LISTE_STATION_INFRAHORAIRE_6M = "/liste-stations/infrahoraire-6m"
      
    @staticmethod
    def _iso(dt_obj: dt.datetime) -> str:
        return dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    @staticmethod
    def chunk_period(start: dt.date, end: dt.date, max_days: int = 366) -> List[Tuple[dt.date, dt.date]]:
        """Split [start, end] into chunks of at most max_days days (inclusive)."""
        chunks = []
        cur = start
        while cur <= end:
            nxt = min(end, cur + dt.timedelta(days=max_days - 1))
            chunks.append((cur, nxt))
            cur = nxt + dt.timedelta(days=1)
        return chunks
    
    @staticmethod
    def combine_csvs(file_list: List[str], out_csv: str) -> pd.DataFrame:
        dfs = []
        for f in file_list:
            try:
                df = pd.read_csv(f, sep=';')
                dfs.append(df)
            except Exception as e:
                #logger.warning(f"Warning: failed to read {f}: {e}")
                pass
        if not dfs:
            return pd.DataFrame()
        big = pd.concat(dfs, ignore_index=True, sort=False)
        big.to_csv(out_csv, index=False)
        return big

    @staticmethod
    def get_with_retry(url, max_retry=5, sleep=10,**kwargs):
        resp = requests.get(url=url, **kwargs)
        for i in range(max_retry):
            status_code = resp.status_code
            if status_code in [200, 201, 202]:
                return resp
            elif status_code==429: #rate limit reached
                #logger.info("Rate limit reached, pausing for 1 min...")
                time.sleep(60)
                resp = requests.get(url=url, **kwargs)
            elif status_code in [500, 502, 503, 504]:
                #logger.info("Request returned error code %i, sleeping for %i seconds...", status_code, sleep)
                time.sleep(10)
                resp = requests.get(url=url, **kwargs)
            else:
                break
        return resp
    
    # helper: ensure base url + path join
    def _url(self, path: str) -> str:
        return self.API_BASE_URL.rstrip("/") + path

    def get_station_metadata(self, station_id: str) -> dict:
        """Call /information-station to get metadata and sensor date ranges."""
        params = {"id-station": station_id}
        r = self.get_with_retry(url=self._url(self.INFORMATION_STATION), headers=self.HEADERS, params=params, timeout=60)
        #r = requests.get(self._url(self.INFORMATION_STATION), headers=self.HEADERS, params=params, timeout=60)
        r.raise_for_status()
        return r.json()[0]

    def scrape_stations_metadata(self, cadence: str, department: int, already_scraped_ids: Set) -> List[Dict]:
        """
        Gets the lsit of all stations for the given department
        cadence: 'horaire' | 'quotidienne' | 'infrahoraire-6m'
        department: french department number (ex: 74 for haute savoie)
        Returns: order_id (string)
        """
        if cadence == "horaire":
            endpoint = self.LISTE_STATION_HORAIRE
        elif cadence == "quotidienne":
            endpoint = self.LISTE_STATION_QUOTIDIENNE
        elif cadence == "6m" or cadence == "infrahoraire-6m":
            endpoint = self.LISTE_STATION_INFRAHORAIRE_6M
        else:
            raise ValueError("Unsupported cadence. Use 'horaire', 'quotidienne' or '6m'/'infrahoraire-6m'.")

        params = {
            "id-departement": str(department)
        }

        r = self.get_with_retry(url=self._url(endpoint), headers={**self.HEADERS, "Content-Type": "application/json"}, params=params, timeout=60)
        #r = requests.get(url=self._url(endpoint), headers={**self.HEADERS, "Content-Type": "application/json"}, params=params, timeout=60)
        r.raise_for_status()
        j = r.json()

        stationsList = []
        #logger.info("Scraping %i stations for department %i", len(j), department)
        for s in tqdm(j, disable=disable_tqdm):
            stationId = s.get('id')
            if (stationId in already_scraped_ids) or not(s.get('posteOuvert')) or (int(stationId)==73187403):
                stationsList.append({"stationId": stationId, "skipped": True, "stationInfo": {}})
            else:
                station_details = self.get_station_metadata(station_id=stationId)

                date_start = station_details.get('dateDebut', '1900-01-01 00:00:00')
                date_end = station_details.get('dateFin') or ''
                paramslist = [{'parameterName': p.get('nom'), 'lastUpdated': self.update_date} for p in station_details.get('parametres', [])]

                station_info = {
                    'stationId': stationId,
                    'name': s.get('nom'),
                    'lon': float(s.get('lon')),
                    'lat': float(s.get('lat')),
                    'altitude': int(s.get('alt')),
                    'lastUpdated': dt.datetime.now(),
                    'dateStart': dt.datetime.strptime(date_start, "%Y-%m-%d %H:%M:%S"),
                    'dateEnd': dt.datetime.strptime('2100-01-01 00:00:00' if date_end=='' else date_end, "%Y-%m-%d %H:%M:%S"),
                    'lastUpdated': self.update_date,
                    'station_parameters': paramslist
                }
                stationsList.append(
                    {"stationId": stationId, 
                     "skipped": False, 
                     "stationInfo": station_info}
                )

                # small sleep to be polite to API (respect rate limits)
                time.sleep(1)
        
        return stationsList

    def place_command(self, cadence: str, station_id: str, start_dt: dt.datetime, end_dt: dt.datetime) -> str:
        """
        Place a command for the desired cadence.
        cadence: 'horaire' | 'quotidienne' | 'infrahoraire-6m'
        Returns: order_id (string)
        """
        if cadence == "horaire":
            endpoint = self.COMMANDE_STATION_HORAIRE
        elif cadence == "quotidienne":
            endpoint = self.COMMANDE_STATION_QUOTIDIENNE
        elif cadence == "6m" or cadence == "infrahoraire-6m":
            endpoint = self.COMMANDE_STATION_INFRAHORAIRE_6M
        else:
            raise ValueError("Unsupported cadence. Use 'horaire', 'quotidienne' or '6m'/'infrahoraire-6m'.")

        params = {
            "id-station": str(station_id),
            "date-deb-periode": self._iso(start_dt),
            "date-fin-periode": self._iso(end_dt)
        }
        # The real API might require a different JSON structure or param names:
        # consult the API doc. Here we post JSON and expect an identifier in the JSON response.
        r = requests.get(self._url(endpoint), headers={**self.HEADERS, "Content-Type": "application/json"}, params=params, timeout=60)
        r.raise_for_status()
        j = r.json()
        # docs indicate response includes something like:
        # {"elaboreProduitAvecDemandeResponse":{"return":"768920711487"}}
        # try to extract robustly:
        order_id = None
        if isinstance(j, dict):
            # walk for a plausible id:
            def find_id(obj):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if isinstance(v, (str, int)) and str(v).isdigit():
                            return str(v)
                        found = find_id(v)
                        if found:
                            return found
                if isinstance(obj, list):
                    for item in obj:
                        found = find_id(item)
                        if found:
                            return found
                return None
            order_id = find_id(j)
        if not order_id:
            raise RuntimeError("Could not extract order id from response: " + str(j))
        return order_id

    def poll_and_download(self, order_id: str, out_dir: str, wait_seconds: int = 5, timeout_minutes: int = 30) -> str:
        """
        Poll the commande/fichier service until the file is ready, then download it.
        Returns the path to the downloaded file.
        """
        deadline = time.time() + timeout_minutes * 60
        status_url = self._url(self.COMMANDE_FICHIER)
        params = {"id-cmde": order_id}
        while True:
            r = requests.get(status_url, headers=self.HEADERS, params=params, timeout=60)
            r.raise_for_status()
            fname = os.path.join(out_dir, f"mf_cmd_{order_id}.csv")
            with open(fname, "wb") as fh:
                fh.write(r.content)
                return fname

            # If the API directly returns file content (sometimes), check headers:
            # If content-type seems like CSV and content is not JSON:
            # We'll call status endpoint without Accept header to see content directly:
            if time.time() > deadline:
                raise TimeoutError(f"Timed out waiting for order {order_id} after {timeout_minutes} minutes.")
            time.sleep(wait_seconds)

    def download_period(self, station_id: str, start_dt: dt.datetime, end_dt: dt.datetime, cadence: str, out_dir: str) -> str:
        """Place command for the given chunk and download the resulting CSV; returns local filename."""
        order_id = self.place_command(cadence, station_id, start_dt, end_dt)
        #logger.info(f"Placed order {order_id} for {start_dt} -> {end_dt} ({cadence})")
        fname = self.poll_and_download(order_id, out_dir)
        #logger.info(f"Downloaded file to {fname}")
        return fname

    def scrape_station(self, 
                       station: str, 
                       start: str , 
                       end: str, 
                       cadence: Literal["horaire","quotidienne","6m","infrahoraire-6m"] = "quotidienne", 
                       out_dir: str = "mf_downloads", 
                       max_chunk_days: int = 366) -> None:

        os.makedirs(out_dir, exist_ok=True)

        start_date = dt.date.fromisoformat(start)
        end_date = dt.date.fromisoformat(end)
        if start_date > end_date:
            raise ValueError("start must be <= end")

        # Build chunks and download each
        chunks = self.chunk_period(start_date, end_date, max_days=max_chunk_days)
        #logger.info(f"Will download {len(chunks)} chunks (max {max_chunk_days} days each).")

        downloaded_files = []
        for sdate, edate in chunks:
            # Convert to datetimes in UTC start of day and end of day (docs require ISO8601 with times)
            start_dt = dt.datetime(sdate.year, sdate.month, sdate.day, 0, 0, 0, tzinfo=dt.timezone.utc)
            # set end at 23:59:59 to include whole day
            end_dt = dt.datetime(edate.year, edate.month, edate.day, 23, 59, 59, tzinfo=dt.timezone.utc)
            try:
                fname = self.download_period(station, start_dt, end_dt, cadence, out_dir)
                downloaded_files.append(fname)
                # small sleep to be polite to API (respect rate limits)
                time.sleep(1)
            except Exception as e:
                #logger.warning(f"Error for chunk {sdate} -> {edate}: {e}")
                # continue to next chunk (partial success is better than failing all)
                continue

        # 3) Combine results
        out_csv = os.path.join(out_dir, f"station_{station}_{start}_to_{end}.csv")
        df = self.combine_csvs(downloaded_files, out_csv)
        #logger.info(f"Combined data saved to {out_csv}. Rows: {len(df)}")

    def _load_stations_metadata(self,
                                engine: Engine,
                                cadence:Literal["horaire","quotidienne","6m","infrahoraire-6m"] = "quotidienne"
                                ) -> None:
        """
        Scrapes all weather stations providing data from the selected granularity metadata 
        """
        already_scraped_stations = load_scraped_stations_ids(engine=engine)
        already_scraped_department = {int(s[:2]) for s in already_scraped_stations}

        #logger.info("loaded %i stations from db", len(already_scraped_stations))
        #logger.info("loaded %i scraped departments from db", len(already_scraped_department))

        scraped_dpt, skipped_dpt = 0, 0
        written, skipped = 0,0
        #logger.info("Scraping weather stations metadata...")
        for dept in range(1,96):
            if dept in already_scraped_department:
                #logger.info("Skipping department %i", dept)
                skipped_dpt += 1 
            else:
                stations = self.scrape_stations_metadata(cadence=cadence, department=dept, already_scraped_ids=already_scraped_stations)
                scraped_dpt += 1
                #logger.info("Writing weather stations metadata in DB...")
                with Session(engine) as session:
                    for stationInfo in stations:
                        if stationInfo["skipped"]:
                            #logging.info("Skipping station with id %s", stationInfo["stationId"])
                            skipped+=1
                        else:
                            insert_weather_station(session=session, commit=False, **stationInfo["stationInfo"])
                            written += 1
                    # Batch writing of weather stations ensuring that all the stations from a given department are written together. 
                    # This is important for the restart logic, which considers that if stations from a given department are already in db, the whole department stations have been scraped and it can be skipped
                    session.commit()
            # FOR DEBUGGING ONLY
            #if dept>=3:
            #    break
        
        #logger.info("Written %i stations for %i departments, skipped %i stations and %i departments", written, scraped_dpt, skipped, skipped_dpt)

        return
    
    
    def run(self) -> None:
        """Entry point for scraper."""
        engine = create_engine(self.DBSTRING)   
        if self.mode == "update":
            pass
        elif self.mode == "init":
            self._load_stations_metadata(engine=engine,
                                         cadence = self.cadence)
       
        else:
            raise ValueError(f"Unsupported mode: {self.mode}")
