"""
Pipeline logger — file + console output.

Usage:
    from src.databricks.utils.logger import get_logger
    log = get_logger(__name__)
    log.info("Pipeline started")
"""

import logging
import logging.handlers
import os
import sys

# ── Config ────────────────────────────────────
LOG_DIR = "d:/PEI/retail-analysis/temp/logs"


_MAX_BYTES = 50 * 1024 * 1024   # 50 MB per file


# ── Formatters ────────────────────────────────
_FILE_FMT = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_CONSOLE_FMT = logging.Formatter(
    fmt="%(levelname)-8s %(asctime)s | %(name)-30s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ── Factory ───────────────────────────────────
_CONFIGURED: set[str] = set()


def get_logger(name: str) -> logging.Logger:
    """Return a logger with file + console handlers (attached once per name)."""
    if name in _CONFIGURED:
        return logging.getLogger(name)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = True

    # File handler — rotating, captures DEBUG+
    os.makedirs(LOG_DIR, exist_ok=True)
    fh = logging.handlers.RotatingFileHandler(
        filename=os.path.join(LOG_DIR, "pipeline.log"),
        maxBytes=_MAX_BYTES,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(_FILE_FMT)
    logger.addHandler(fh)

    # Console handler — INFO+ only
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(_CONSOLE_FMT)
    logger.addHandler(ch)

    _CONFIGURED.add(name)
    return logger
