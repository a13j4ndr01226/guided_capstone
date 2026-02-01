"""
logger_config.py

Module-aware logging factory for data pipelines.

Usage:
    from utils.logger_config import get_logger

    log = get_logger("extract")  # -> logs/YYYY-MM-DD/extract_YYYY-MM-DD.log
    log.info("Pulled 3,348 rows")

    # Optionally provide run context and custom log base
    log = get_logger(
        module_name="transform",
        run_date=None,                # defaults to today
        base_dir=None,                # defaults to env LOG_BASE_DIR or "logs"
        level=None,                   # defaults to env LOG_LEVEL or INFO
    )
"""
from __future__ import annotations
import os
import logging
from pathlib import Path
from datetime import date
from typing import Optional, Dict

# ---------------- constants ----------------
DATE_FMT = "%Y-%m-%d"

_LOGGER_CACHE: Dict[str, logging.Logger] = {}

# -------- helpers --------
def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _get_env_level(default: int = logging.INFO) -> int:
    name = os.getenv("LOG_LEVEL", "").upper()
    return getattr(logging, name, default)

class _ContextFilter(logging.Filter):
    """
    Injects stable fields into every record so the formatter can reference them.
    Values can also be supplied via env vars (nice for orchestrators):
        RUN_ID, BATCH_ID
    """
    def __init__(self, module_name: str, run_date: str, run_id: Optional[str] = None, batch_id: Optional[str] = None):
        super().__init__()
        self.module_name = module_name
        self.run_date = run_date
        self.run_id = run_id or os.getenv("RUN_ID") or "-"
        self.batch_id = batch_id or os.getenv("BATCH_ID") or "-"

    def filter(self, record: logging.LogRecord) -> bool:
        record.module_name = self.module_name
        record.run_date = self.run_date
        record.run_id = self.run_id
        record.batch_id = self.batch_id
        return True

# -------- public API --------
def get_logger(
            module_name: str,
            *,
            run_date: Optional[date] = None,
            base_dir: Optional[str] = None,
            level: Optional[int] = None,
            propagate: bool = False,
        ) -> logging.Logger:
    """
    Return a configured logger for a pipeline module.

    Creates logs in: {base_dir}/{YYYY-MM-DD}/{module_name}_{YYYY-MM-DD}.log
    and attaches a console handler. Safe to call multiple times; handlers
    are added once per (module_name).

    Args:
        module_name: Short name of the pipeline stage or script running(e.g., "extract", "transform", "load")
        run_date:    Date to stamp logs; defaults to today
        base_dir:    Root logs dir; defaults to $LOG_BASE_DIR or "logs"
        level:       Log level; defaults to $LOG_LEVEL or logging.INFO
        propagate:   Set to True only if you want messages to bubble to parent loggers

    Returns:
        logging.Logger
    """
    if not module_name or not module_name.strip():
        raise ValueError("module_name must be a non-empty string")

    name = f"pipeline.{module_name}"
    if name in _LOGGER_CACHE:
        return _LOGGER_CACHE[name]

    # Resolve configuration
    base_dir = base_dir or os.getenv("LOG_BASE_DIR", "logs")
    log_level = level if level is not None else _get_env_level()
    d = run_date or date.today()
    date_str = d.strftime(DATE_FMT)

    # paths
    dated_dir = Path(base_dir) / date_str
    _ensure_dir(dated_dir)
    logfile = dated_dir / f"{module_name}_{date_str}.log"

    # build logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.propagate = propagate

    # Reduce Spark noise
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

    # formatter with context fields
    # example line:
    # 2025-08-28 12:34:56 INFO [extract] [run:abc123] Pulled 3348 rows
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(module_name)s] [%(run_date)s] : %(message)s"
    )

    # context filter (module/run ids)
    logger.addFilter(_ContextFilter(module_name, date_str))

    if not logger.handlers:
        # file handler
        fh = logging.FileHandler(logfile, encoding="utf-8")
        fh.setLevel(log_level)
        fh.setFormatter(formatter)

        # console handler
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)

    _LOGGER_CACHE[name] = logger
    return logger

def get_run_log_dir(
    run_date: Optional[date] = None,
    base_dir: Optional[str] = None,
) -> Path:
    """
    Convenience for assembling the dated log folder path.

    Example:
        run_dir = get_run_log_dir()
        # -> Path('logs/2025-08-28')
    """
    base_dir = base_dir or os.getenv("LOG_BASE_DIR", "logs")
    d = run_date or date.today()
    return Path(base_dir) / d.strftime(DATE_FMT)


