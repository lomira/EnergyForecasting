"""Generate hourly Algerian public-holiday flags."""

from datetime import datetime
from pathlib import Path

import holidays
import pandas as pd
from loguru import logger

from holiday_data.store import DB_PATH, _upsert


def sync(
    from_date: datetime,
    to_date: datetime,
    *,
    db_path: Path = DB_PATH,
) -> int:
    """Generate and upsert one holiday flag per hour in the requested range."""
    if from_date > to_date:
        raise ValueError("from_date must not be after to_date")
    country_holidays = holidays.country_holidays(
        "DZ", years=range(from_date.year, to_date.year + 1)
    )
    holiday_dates = set(country_holidays)
    records = [
        (timestamp.isoformat(sep=" "), int(timestamp.date() in holiday_dates))
        for timestamp in pd.date_range(from_date, to_date, freq="h")
    ]
    _upsert(records, db_path=db_path)
    logger.info(
        f"Stored {len(records):,.0f} holiday flags between {from_date} and {to_date}"
    )
    return len(records)
