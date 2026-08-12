from datetime import datetime

import openmeteo_requests
import pandas as pd
import requests_cache
from django.conf import settings
from django.db import connection, transaction
from retry_requests import retry

from engine.logging_config import logger, timed
from engine.models import WeatherObservation, weather_api_params


def _log_cache_hit(response, *args, **kwargs):
    """Log whether an Open-Meteo response was served from cache or made a live API call."""
    status = "CACHE HIT" if getattr(response, "from_cache", False) else "API CALL"
    logger.debug(f"[{status}] {response.request.method} {response.request.url}")
    return response


# The insert bulk from native django ORM is not efficient enough for the large number of weather observations, so we use a raw SQL upsert instead.
# This is compatible with SQLite and PostgreSQL, but may need adjustments for DuckDB
# On my machine the native orm took around 4.5 sec whereas the raw SQL upsert took around 0.5 sec
# In the end, it might be better to keep the native way if the 4 secondes are not a problem,
# because it is more readable and maintainable.
# But for now, we keep the raw SQL upsert for performance reaso, as I recreate the database each time


def _upsert_weather(rows: dict, metrics: list[str]) -> None:
    columns = ["datetime", "city", *metrics]
    quote = connection.ops.quote_name
    sql = (
        f"INSERT INTO {quote(WeatherObservation._meta.db_table)} "
        f"({', '.join(map(quote, columns))}) "
        f"VALUES ({', '.join(['%s'] * len(columns))}) "
        f"ON CONFLICT ({quote('datetime')}, {quote('city')}) DO UPDATE SET "
        + ", ".join(f"{quote(name)} = excluded.{quote(name)}" for name in metrics)
    )
    records = (
        (
            str(row["datetime"]),
            row["city"],
            *(None if pd.isna(row.get(name)) else float(row[name]) for name in metrics),
        )
        for row in rows.values()
    )
    with transaction.atomic(), connection.cursor() as cursor:
        cursor.executemany(sql, records)


def get_weather_data(from_date: datetime, to_date: datetime) -> None:
    """Fetch weather from Open-Meteo and store it as one wide row per timestamp."""
    cache_session = requests_cache.CachedSession(
        settings.ENGINE_CACHE_METEO,
        expire_after=-1,  # Never
    )
    cache_session.hooks["response"].append(_log_cache_hit)

    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://previous-runs-api.open-meteo.com/v1/forecast"

    # Only the variables declared as columns on WeatherObservation are queried.
    hourly_requests = weather_api_params()

    # Accumulate one wide row per (timestamp, city).
    rows: dict[tuple[datetime, str], dict] = {}
    for _, ville in settings.ENGINE_VILLES.items():
        params = {
            "latitude": ville["lat"],
            "longitude": ville["lon"],
            "hourly": hourly_requests,
            "start_date": from_date.strftime("%Y-%m-%d"),
            "end_date": to_date.strftime("%Y-%m-%d"),
        }

        with timed(f"Fetch weather for {ville['name']}"):
            responses = openmeteo.weather_api(url, params=params)
            hourly = responses[0].Hourly()

            datetimes = pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            ).tz_localize(None)

            # Map each API variable to its metric column on the model.
            for index, api_param in enumerate(hourly_requests):
                values = hourly.Variables(index).ValuesAsNumpy()
                for ts, value in zip(datetimes, values):
                    key = (ts, ville["name"])
                    row = rows.setdefault(key, {"datetime": ts, "city": ville["name"]})
                    row[api_param] = value

    with timed("bulk insert weather observations"):
        _upsert_weather(rows, hourly_requests)
    logger.info(f"Stored {len(rows):,.0f} weather observations")
