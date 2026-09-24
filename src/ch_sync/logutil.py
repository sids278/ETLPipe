"""Logging setup shared by the supervisor and worker processes."""
from __future__ import annotations

import logging
import sys


def setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)-7s %(processName)-18s %(name)s: %(message)s")
    )
    root.addHandler(handler)
    root.setLevel(level.upper())
    for noisy in ("urllib3", "clickhouse_connect"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
