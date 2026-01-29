import argparse
import logging
import time

from willthisfreeze.scraper import weather_stations_attribution
from willthisfreeze.config.logging_config import configure_logging, set_log_context

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    run_id, listener = configure_logging()
    try:
        parser = argparse.ArgumentParser(
            prog="Weather stations attribution",
            description="updates the mapping between routes and weather stations",
        )
        parser.add_argument(
            "-m", "--mode",
            choices=["update", "reset"],
            default="update",
            required=False,
        )
        args = parser.parse_args()

        # Context for all subsequent log lines
        set_log_context(component="weather_stations_attribution", mode=args.mode)

        logger.info("app.start", extra={"run_id": run_id, "mode": args.mode})
        start_time = time.time()

        logger.info("attribution.start")
        weather_stations_attribution(mode=args.mode)
        elapsed = time.time() - start_time

        logger.info("attribution.done", extra={"duration_s": round(elapsed, 2)})
        logger.info("app.done")

    except Exception:
        logger.exception("app.crash")
        raise
    finally:
        listener.stop()
