import sys
import time
import logging
import argparse

from willthisfreeze.scraper.weather_stations_attribution import main_weather_stations_attribution

# -----------------------
# Logging configuration
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("DataPreparation")


# -----------------------
# CLI entrypoint
# -----------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="Weather stations attribution",
        description="updates the mapping between routes and weather stations",
    )
    parser.add_argument("-m", "--mode", choices=["update", "reset"], default="update", required=False)

    args = parser.parse_args()

    logger.info("Starting weather stations attribution is mode %s", args.mode)
    start_time = time.time()

    main_weather_stations_attribution(mode=args.mode)

    logger.info("Finished in %.2f seconds", time.time() - start_time)

