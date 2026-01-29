import json
import logging
import os
import queue
import socket
import sys
import time
import uuid
from contextvars import ContextVar
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler

run_id_var: ContextVar[str] = ContextVar("run_id", default="-")
component_var: ContextVar[str] = ContextVar("component", default="-")
mode_var: ContextVar[str] = ContextVar("mode", default="-")

_HOSTNAME = socket.gethostname()


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = run_id_var.get()
        record.component = component_var.get()
        record.mode = mode_var.get()
        record.hostname = _HOSTNAME
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "run_id": getattr(record, "run_id", "-"),
            "component": getattr(record, "component", "-"),
            "mode": getattr(record, "mode", "-"),
            "hostname": getattr(record, "hostname", _HOSTNAME),
        }
        # include extras (safe-ish best effort)
        for k, v in record.__dict__.items():
            if k.startswith("_") or k in payload:
                continue
            if k in ("msg", "args", "levelname", "name", "created", "msecs", "relativeCreated"):
                continue
            try:
                json.dumps(v)
                payload[k] = v
            except TypeError:
                payload[k] = repr(v)

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


def set_log_context(*, run_id: str | None = None, component: str | None = None, mode: str | None = None) -> None:
    if run_id is not None:
        run_id_var.set(run_id)
    if component is not None:
        component_var.set(component)
    if mode is not None:
        mode_var.set(mode)


def configure_logging() -> tuple[str, QueueListener]:
    """
    Central logging config. Call once in an entrypoint.
    Returns (run_id, listener). Stop listener on exit.
    """
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_json = os.getenv("LOG_JSON", "1") == "1"  # default: JSON-friendly for servers
    log_to_file = os.getenv("LOG_TO_FILE", "0") == "1"
    log_dir = os.getenv("LOG_DIR", "logs")

    run_id = os.getenv("RUN_ID") or str(uuid.uuid4())
    set_log_context(run_id=run_id)

    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers.clear()

    context_filter = ContextFilter()

    if log_json:
        formatter: logging.Formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s run=%(run_id)s component=%(component)s mode=%(mode)s %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    # Real outputs (listener side)
    outputs: list[logging.Handler] = []

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    sh.addFilter(context_filter)
    outputs.append(sh)

    if log_to_file:
        os.makedirs(log_dir, exist_ok=True)

        fh = RotatingFileHandler(
            os.path.join(log_dir, "app.log"),
            maxBytes=10_000_000,
            backupCount=5,
            encoding="utf-8",
        )
        fh.setFormatter(formatter)
        fh.addFilter(context_filter)
        outputs.append(fh)

        eh = RotatingFileHandler(
            os.path.join(log_dir, "error.log"),
            maxBytes=10_000_000,
            backupCount=5,
            encoding="utf-8",
        )
        eh.setLevel(logging.WARNING)
        eh.setFormatter(formatter)
        eh.addFilter(context_filter)
        outputs.append(eh)

    # Queue-based root handler (safer if you add concurrency later)
    q: queue.Queue = queue.Queue(-1)
    root.addHandler(QueueHandler(q))

    listener = QueueListener(q, *outputs, respect_handler_level=True)
    listener.start()

    # Reduce some libraries logging level (they log a lot at INFO)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    return run_id, listener
