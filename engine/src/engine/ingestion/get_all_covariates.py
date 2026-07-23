from datetime import datetime

import pandas as pd

from engine.models import Holiday, WeatherObservation, weather_api_params


def get_all_covariates(from_date: datetime, to_date: datetime) -> pd.DataFrame:
    # WEATHER

    # Weather is stored wide per (datetime, city); pivot to "<city>_<metric>" columns.
    weather_rows = WeatherObservation.objects.filter(
        datetime__range=(from_date, to_date)
    ).values("datetime", "city", *weather_api_params())
    weather_long = pd.DataFrame.from_records(weather_rows)
    if weather_long.empty:
        raise ValueError(
            f"No weather observations found between {from_date} and {to_date}"
        )
    weather_tidy = weather_long.pivot_table(
        index="datetime",
        columns="city",
        values=weather_api_params(),
        aggfunc="first",
    ).sort_index()
    # Flatten the (metric, city) MultiIndex into "<city>_<metric>" column names.
    weather_tidy.columns = [f"{city}_{metric}" for metric, city in weather_tidy.columns]

    # HOLYDAYS
    holiday_rows = Holiday.objects.filter(
        datetime__range=(from_date, to_date)
    ).values_list("datetime", "is_holiday")
    holidays_df = pd.DataFrame(
        list(holiday_rows), columns=["datetime", "holidays"]
    ).set_index("datetime")

    return pd.concat([weather_tidy, holidays_df], axis=1, join="outer")
