"""Fetch weather observations from Open-Meteo."""

from datetime import datetime
from pathlib import Path

import openmeteo_requests
import pandas as pd
import requests_cache
from loguru import logger
from retry_requests import retry

from weather_data.config import (
    CACHE_PATH,
    CITIES,
    DB_PATH,
    WEATHER_API_PARAMS,
)
from weather_data.store import _upsert


def _log_cache_hit(response, *args, **kwargs):
    status = "CACHE HIT" if getattr(response, "from_cache", False) else "API CALL"
    logger.debug(f"[{status}] {response.request.method} {response.request.url}")
    return response


def sync(
    from_date: datetime,
    to_date: datetime,
    *,
    db_path: Path = DB_PATH,
    cache_path: Path = CACHE_PATH,
) -> int:
    """Fetch Open-Meteo observations and upsert them into the weather database."""
    if from_date > to_date:
        raise ValueError("from_date must not be after to_date")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_session = requests_cache.CachedSession(str(cache_path), expire_after=-1)
    cache_session.hooks["response"].append(_log_cache_hit)
    openmeteo = openmeteo_requests.Client(
        session=retry(cache_session, retries=5, backoff_factor=0.2)
    )

    rows: dict[tuple[pd.Timestamp, str], dict[str, object]] = {}
    for city in CITIES:
        responses = openmeteo.weather_api(
            "https://previous-runs-api.open-meteo.com/v1/forecast",
            params={
                "latitude": city["lat"],
                "longitude": city["lon"],
                "hourly": WEATHER_API_PARAMS,
                "start_date": from_date.strftime("%Y-%m-%d"),
                "end_date": to_date.strftime("%Y-%m-%d"),
            },
        )
        hourly = responses[0].Hourly()
        datetimes = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        ).tz_localize(None)
        for index, api_param in enumerate(WEATHER_API_PARAMS):
            for timestamp, value in zip(
                datetimes, hourly.Variables(index).ValuesAsNumpy()
            ):
                key = (timestamp, str(city["name"]))
                row = rows.setdefault(
                    key, {"datetime": timestamp, "city": city["name"]}
                )
                row[api_param] = value

    _upsert(rows, db_path=db_path)
    logger.info(f"Stored {len(rows):,.0f} weather observations")
    return len(rows)
