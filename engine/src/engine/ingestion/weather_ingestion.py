from datetime import datetime

import openmeteo_requests
import pandas as pd
import requests_cache
from django.conf import settings
from retry_requests import retry

from engine.models import WeatherObservation


def build_weather_column_names(
    weather_metrics: list[str] | None = None,
    previous_days: int | None = None,
) -> list[str]:
    """Build the Open-Meteo hourly request param names from the metric list.

    e.g. ["temperature_2m"] with previous_days=2 ->
    ["temperature_2m", "temperature_2m_previous_day1", "temperature_2m_previous_day2"].
    """
    metrics = weather_metrics or []
    suffixes = [""]
    if (previous_days or 0) > 0:
        suffixes += [f"_previous_day{i + 1}" for i in range(previous_days or 0)]
    return [f"{metric}{suffix}" for metric in metrics for suffix in suffixes]


def get_weather_data(from_date: datetime, to_date: datetime) -> None:
    """Get the weather data from the Open-Meteo API and store it as EAV rows."""
    # TODO Change this to a wider model as it it way to long to write on disk.

    cache_session = requests_cache.CachedSession(
        settings.ENGINE_CACHE_METEO,
        expire_after=-1,  # Never
    )

    # TODO Move this to loguru sometime
    def _log_cache_hit(response, *args, **kwargs):
        status = "CACHE HIT" if getattr(response, "from_cache", False) else "API CALL"
        print(f"  [{status}] {response.request.method} {response.request.url}")
        return response

    cache_session.hooks["response"].append(_log_cache_hit)

    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://previous-runs-api.open-meteo.com/v1/forecast"

    # List all the variables we will querry
    hourly_requests = build_weather_column_names(
        weather_metrics=settings.ENGINE_WEATHER_METRICS,
        previous_days=settings.ENGINE_WEATHER_PREV_DAYS,
    )

    # Process weather data per city, melting each wide frame to long EAV rows.
    observations: list[WeatherObservation] = []
    for _, ville in settings.ENGINE_VILLES.items():
        params = {
            "latitude": ville["lat"],
            "longitude": ville["lon"],
            "hourly": hourly_requests,
            "start_date": from_date.strftime("%Y-%m-%d"),
            "end_date": to_date.strftime("%Y-%m-%d"),
        }

        responses = openmeteo.weather_api(url, params=params)
        hourly = responses[0].Hourly()

        # Build base dictionary with the datetime index
        hourly_data = {
            "datetime": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )
        }

        # Map API variables straight to columns using the exact same request names
        for index, col_name in enumerate(hourly_requests):
            hourly_data[col_name] = hourly.Variables(index).ValuesAsNumpy()

        # Clean up dataframe styling and timezones
        hourly_dataframe = pd.DataFrame(data=hourly_data).set_index("datetime")
        hourly_dataframe.index = hourly_dataframe.index.tz_localize(None)
        hourly_dataframe = hourly_dataframe.reset_index().rename(
            columns={"index": "datetime"}
        )
        hourly_dataframe["city"] = ville["name"]

        # Melt wide -> long (datetime, city, metric, value) for the EAV model.
        long = hourly_dataframe.melt(
            id_vars=["datetime", "city"], var_name="metric", value_name="value"
        )
        observations.extend(
            WeatherObservation(
                datetime=row["datetime"],
                city=row["city"],
                metric=row["metric"],
                value=row["value"],
            )
            for row in long.to_dict("records")
        )

        print(f"Processed hourly data for {ville['name']}")

    WeatherObservation.objects.bulk_create(
        observations,
        update_conflicts=True,
        unique_fields=["datetime", "city", "metric"],
        update_fields=["value"],
    )
