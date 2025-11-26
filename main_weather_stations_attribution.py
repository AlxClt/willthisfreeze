import sys
import time
import logging

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

    logger.info("Starting weather stations attribution")
    start_time = time.time()

    main_weather_stations_attribution()

    logger.info("Finished in %.2f seconds", time.time() - start_time)

