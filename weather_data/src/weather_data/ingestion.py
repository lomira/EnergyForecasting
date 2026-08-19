"""Fetch weather observations from Open-Meteo."""

from collections.abc import Mapping
from pathlib import Path
from typing import cast

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
    WEATHER_METRICS,
)
from weather_data.store import _upsert


def _log_cache_hit(response, *args, **kwargs):
    status = "CACHE HIT" if getattr(response, "from_cache", False) else "API CALL"
    logger.debug(f"[{status}] {response.request.method} {response.request.url}")
    return response


def _fetch_source(
    openmeteo: openmeteo_requests.Client,
    url: str,
    model: str,
    city: Mapping[str, object],
    api_params: tuple[str, ...],
    from_date: pd.Timestamp,
    to_date: pd.Timestamp,
) -> pd.DataFrame:
    hourly = openmeteo.weather_api(
        url,
        params={
            "latitude": city["lat"],
            "longitude": city["lon"],
            "models": model,
            "hourly": api_params,
            "start_date": from_date.strftime("%Y-%m-%d"),
            "end_date": to_date.strftime("%Y-%m-%d"),
        },
    )[0].Hourly()
    if hourly is None:
        raise ValueError("No Data received")
    datetimes = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    ).tz_localize(None)
    return pd.DataFrame(
        {
            api_param: hourly.Variables(index).ValuesAsNumpy()  # ty: ignore[unresolved-attribute]
            for index, api_param in enumerate(api_params)
        },
        index=datetimes,
    )


def _fetch_rows(
    from_date: pd.Timestamp,
    to_date: pd.Timestamp,
    cache_path: Path,
) -> dict[tuple[pd.Timestamp, str], dict[str, object]]:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_session = requests_cache.CachedSession(str(cache_path), expire_after=-1)
    cache_session.hooks["response"].append(_log_cache_hit)
    openmeteo = openmeteo_requests.Client(
        session=retry(cache_session, retries=5, backoff_factor=0.2)  # ty: ignore[invalid-argument-type]
    )
    rows: dict[tuple[pd.Timestamp, str], dict[str, object]] = {}
    sources = [
        (
            "https://archive-api.open-meteo.com/v1/archive",
            "era5",
            WEATHER_METRICS,
            from_date,
        )
    ]
    # Previous runs are only available from 2024 onwards.
    previous_runs_from = max(from_date, pd.Timestamp("2024-01-01"))
    if previous_runs_from <= to_date:
        sources.append(
            (
                "https://previous-runs-api.open-meteo.com/v1/forecast",
                "ecmwf_ifs025",
                tuple(
                    param
                    for param in WEATHER_API_PARAMS
                    if param not in WEATHER_METRICS
                ),
                previous_runs_from,
            )
        )
    for city in CITIES:
        for url, model, api_params, source_from in sources:
            if type(source_from) is not pd.Timestamp:
                raise TypeError(f"Expected pd.Timestamp, got {type(source_from)}")
            for timestamp, values in _fetch_source(
                openmeteo, url, model, city, api_params, source_from, to_date
            ).iterrows():
                timestamp = cast(pd.Timestamp, timestamp)
                key = (timestamp, str(city["name"]))
                row = rows.setdefault(
                    key, {"datetime": timestamp, "city": city["name"]}
                )
                row.update(values.to_dict())
    return rows


def sync(
    from_date: pd.Timestamp,
    to_date: pd.Timestamp,
    *,
    db_path: Path = DB_PATH,
    cache_path: Path = CACHE_PATH,
) -> int:
    """Fetch Open-Meteo observations and upsert them into the weather database."""
    if from_date > to_date:
        raise ValueError("from_date must not be after to_date")
    rows = _fetch_rows(from_date, to_date, cache_path)

    _upsert(rows, db_path=db_path)
    logger.info(f"Stored {len(rows):,.0f} weather observations")
    return len(rows)
