import argparse
import logging
import sys
import time
import datetime as dt

from willthisfreeze.scraper.utils import read_config, read_secret
from willthisfreeze.scraper import MFScraper

# -----------------------
# Logging configuration
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("MFScraper")


# -----------------------
# CLI entrypoint
# -----------------------
if __name__ == "__main__":

    p = argparse.ArgumentParser()
    p.add_argument("--mode", required=False, help="scraping mode", choices=["load_stations", "update_weather_hist"], default="update_weather_hist")
    args = p.parse_args()

    conf = read_config()
    secret = read_secret()
    conf.update(secret)
    
    start_time = time.time()

    scraper = MFScraper(config=conf, mode=args.mode)
    scraper.run()

    logger.info("Finished in %.2f seconds", time.time() - start_time)
