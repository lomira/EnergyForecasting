from datetime import datetime

import openmeteo_requests
import pandas as pd
import requests_cache
from django.conf import settings
from retry_requests import retry

from engine.logging_config import logger, timed
from engine.models import WeatherObservation, weather_api_params


def _log_cache_hit(response, *args, **kwargs):
    """Log whether an Open-Meteo response was served from cache or made a live API call."""
    status = "CACHE HIT" if getattr(response, "from_cache", False) else "API CALL"
    logger.debug(f"[{status}] {response.request.method} {response.request.url}")
    return response


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

    observations = [WeatherObservation(**row) for row in rows.values()]
    with timed("bulk insert weather observations"):
        WeatherObservation.objects.bulk_create(
            observations,
            update_conflicts=True,
            unique_fields=["datetime", "city"],
            update_fields=weather_api_params(),
        )
    logger.info(f"Stored {len(observations):,.0f} weather observations")
