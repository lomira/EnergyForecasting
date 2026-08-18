import logging
import sys
import warnings
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
    logging.getLogger("lightning_fabric").setLevel(logging.ERROR)
    logging.getLogger("lightning_utilities").setLevel(logging.ERROR)
    logging.getLogger("pytorch_lightning").setLevel(logging.ERROR)
    warnings.filterwarnings(
        "ignore",
        message=r"`isinstance\(treespec, LeafSpec\)` is deprecated",
        module=r"pytorch_lightning\.utilities\._pytree",
    )
    warnings.filterwarnings(
        "ignore",
        message=r"Total length of `list` across ranks is zero\.",
        module=r"pytorch_lightning\.utilities\.data",
    )

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
