import sys
from pathlib import Path

from loguru import logger

# Rotating log file lives under <workspace>/data/engine.log
LOG_FILE = Path(__file__).resolve().parents[3] / "data" / "engine.log"

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
