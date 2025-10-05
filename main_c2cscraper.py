import argparse
import logging
import sys
import time

from willthisfreeze.scraper.utils import read_config
from willthisfreeze.scraper import C2CScraper
from willthisfreeze.dbutils import create_local_db

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
        prog="c2cScraper",
        description="Scrapes camptocamp.org routes and outings data",
    )
    parser.add_argument("mode", choices=["initial_load", "update"])

    args = parser.parse_args()

    conf = read_config()

    logger.info("Starting scraper in %s mode", args.mode)
    start_time = time.time()

    if args.mode=='initial_load':
        # initial_load can be called without existing db. If db exists, this will have no effect
        create_local_db()

    scraper = C2CScraper(mode=args.mode, config=conf)
    message = scraper.run()

    logger.info("Finished in %.2f seconds", time.time() - start_time)
    logger.info("Summary: %s", message)