import sqlite3
from datetime import datetime

import pandas as pd

from engine.config.config import settings
from engine.database.manage_conn import read_table_as_df


def get_all_covariates(from_date: datetime, to_date: datetime) -> pd.DataFrame:
    with sqlite3.connect(str(settings.sqlite_path)) as con:
        weather_df = read_table_as_df(
            con,
            settings.tables.weather,
            from_date,
            to_date,
            parse_dates="datetime",
        )
        holidays_df = read_table_as_df(
            con,
            settings.tables.holidays,
            from_date,
            to_date,
            parse_dates="datetime",
        )

    weather_tidy = (
        weather_df.reset_index()
        .pivot_table(
            index="datetime",
            columns="city",
            values=weather_df.columns.difference(["datetime", "city"]).tolist(),
            aggfunc="first",
        )
        .sort_index()
    )
    weather_tidy.columns = [f"{city}_{metric}" for metric, city in weather_tidy.columns]

    holidays_df = holidays_df.rename(columns={"holidays": "holidays"})

    return pd.concat([weather_tidy, holidays_df], axis=1, join="inner")
