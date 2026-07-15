import sqlite3

import openmeteo_requests
import pandas as pd
import requests_cache
from engine.config.config import settings
from engine.data_model.weather_model import build_weather_column_names
from engine.database.manage_conn import add_rows
from retry_requests import retry


def get_weather_data() -> None:
    """Get the weather data from the Open-Meteo API and save it to a CSV file."""

    cache_session = requests_cache.CachedSession(settings.cache_meteo, expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://previous-runs-api.open-meteo.com/v1/forecast"

    # One single list handles both the API request and the DataFrame columns
    hourly_requests = build_weather_column_names(
        weather_metrics=settings.weather_metrics,
        previous_days=settings.weather_metrics_previous_days,
    )

    # Process weather data per city
    for _, ville in settings.ville.items():
        params = {
            "latitude": ville.lat,
            "longitude": ville.lon,
            "hourly": hourly_requests,
            "start_date": "2019-01-01",
            "end_date": "2020-02-01",
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
        hourly_dataframe["city"] = ville.name

        with sqlite3.connect(str(settings.sqlite_path)) as con:
            add_rows(con, "weather", hourly_dataframe)

        print(f"Processed hourly data for {ville.name}")
