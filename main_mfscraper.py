import argparse
import logging
import time

from willthisfreeze.config import read_config, read_secret
from willthisfreeze.scraper import MFScraper
from willthisfreeze.config.logging_config import configure_logging, set_log_context

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    run_id, listener = configure_logging()
    try:
        p = argparse.ArgumentParser()
        p.add_argument(
            "--mode",
            required=False,
            help="scraping mode",
            choices=["init", "update"],
            default="update",
        )
        args = p.parse_args()

        # Context for all subsequent log lines
        set_log_context(component="meteofrance", mode=args.mode)

        logger.info("app.start", extra={"run_id": run_id, "mode": args.mode})

        conf = read_config()
        secret = read_secret()
        conf.update(secret)

        start_time = time.time()

        scraper = MFScraper(config=conf, mode=args.mode)
        logger.info("scraper.start")
        scraper.run()

        elapsed = time.time() - start_time
        logger.info("scraper.done", extra={"duration_s": round(elapsed, 2)})
        logger.info("app.done")

    except Exception:
        logger.exception("app.crash")
        raise
    finally:
        listener.stop()