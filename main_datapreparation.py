import argparse
import logging
import sys
import time

from willthisfreeze.datapreparation.weather_stations_attribution import main_weather_stations_attribution

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
        prog="datapreparation",
        description="Executes the various data p^reparation tasks",
    )
    parser.add_argument("task", choices=["weather_stations_attribution",])

    args = parser.parse_args()

   

    logger.info("Starting data preparation task %s", args.task)
    start_time = time.time()

    # TODO: enhance maybe with a unique entry point for all data preparation tasks + implement logic to update only routes without already attributed stations (except if forced update)
    if args.task=='weather_stations_attribution':
        main_weather_stations_attribution()

    logger.info("Finished in %.2f seconds", time.time() - start_time)

