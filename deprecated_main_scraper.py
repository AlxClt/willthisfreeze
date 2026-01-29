import sys
import time
import logging
import argparse

from willthisfreeze.dbutils import create_local_db
from willthisfreeze.config import read_config, read_secret
from willthisfreeze.scraper import C2CScraper, MFScraper, weather_stations_attribution

# -----------------------
# Logging configuration
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("Scraper")


# -----------------------
# CLI entrypoint
# -----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Scraper",
        description="Scrapes camptocamp.org and meteofrance data",
    )
    parser.add_argument("mode", choices=["init", "update"])
    parser.add_argument("--reset-weather-stations-attribution", action='store_true', help="wether to rerun the weather station attribution to routes")

    args = parser.parse_args()

    conf = read_config()
    secret = read_secret()
    conf.update(secret)

    logger.info("Starting scraper in %s mode", args.mode)

    if args.mode=='init':
        # initial_load can be called without existing db. If db exists, this will have no effect
        create_local_db()

    start_time = time.time()

    logger.info("Starting C2C scraping")

    c2c_scraper = C2CScraper(mode=args.mode, config=conf)
    message = c2c_scraper.run()

    logger.info("C2C scraping finished in %.2f seconds", time.time() - start_time)
    logger.info("Summary: %s", message)


    stations_attribution_start_time = time.time()

    logger.info("Starting Weather stations attribution")

    if args.reset_weather_stations_attribution:
        weather_stations_attribution(mode='reset')
    else:
        weather_stations_attribution(mode='update')

    logger.info("Weather stations attribution finished in %.2f seconds", time.time() - stations_attribution_start_time)


    mfscraper_start_time = time.time()

    logger.info("Starting MeteoFrance scraping")

    MFscraper = MFScraper(config=conf, mode=args.mode)
    MFscraper.run()

    logger.info("MeteoFrance scraping finished in %.2f seconds", time.time() - mfscraper_start_time)

    logger.info("Scraping finished in %.2f seconds", time.time() - start_time)

