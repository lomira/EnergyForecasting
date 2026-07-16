from datetime import datetime

import pandas as pd

from engine.models import Holiday, WeatherObservation


def get_all_covariates(from_date: datetime, to_date: datetime) -> pd.DataFrame:
    # Weather: pivot long -> wide creating columns <city>_<metric>"
    weather_rows = WeatherObservation.objects.filter(
        datetime__range=(from_date, to_date)
    ).values("datetime", "city", "metric", "value")
    weather_long = pd.DataFrame.from_records(weather_rows)
    if weather_long.empty:
        raise ValueError(
            f"No weather observations found between {from_date} and {to_date}"
        )
    weather_tidy = weather_long.pivot_table(
        index="datetime",
        columns=["city", "metric"],
        values="value",
        aggfunc="first",
    ).sort_index()
    weather_tidy.columns = [
        f"{city}_{metric}" for city, metric in weather_tidy.columns
    ]

    # Holidays
    holiday_rows = Holiday.objects.filter(
        datetime__range=(from_date, to_date)
    ).values_list("datetime", "is_holiday")
    holidays_df = pd.DataFrame(
        list(holiday_rows), columns=["datetime", "holidays"]
    ).set_index("datetime")

    return pd.concat([weather_tidy, holidays_df], axis=1, join="inner")
