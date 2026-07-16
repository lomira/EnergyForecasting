import sys
import time
from contextlib import contextmanager

from loguru import logger

from engine.django_settings import ENGINE_DATA_ROOT

# Rotating log file lives under <workspace>/data/engine.log
LOG_FILE = ENGINE_DATA_ROOT / "engine.log"


@contextmanager
def timed(label: str, level: str = "DEBUG"):
    """Log how long a block of code takes.

    Usage::

        with timed("fetch weather for Alger"):
            do_work()

    Emits a single log line like ``"fetch weather for Alger took 1.23s"``.
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        logger.log(level, f"{label} took {elapsed:.2f}s")


def setup_logging(
    *,
    level: str = "DEBUG",
    log_file: bool = True,
) -> None:
    """Configure loguru handlers."""
    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{module}</cyan>:<cyan>{function}</cyan> - {message}"
        ),
    )

    if log_file:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        logger.add(
            str(LOG_FILE),
            level=level,
            rotation="10 MB",
            retention=5,
            enqueue=True,
            format=(
                "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
                "{module}:{function}:{line} - {message}"
            ),
        )
