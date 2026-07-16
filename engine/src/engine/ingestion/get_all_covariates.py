import sqlite3
from datetime import datetime

import pandas as pd

from engine.config.config import settings
from engine.database.manage_conn import read_table_as_df


def get_all_covariates(from_date: datetime, to_date: datetime) -> pd.DataFrame:
    all_tables = [
        settings.tables.weather_tidy,
        settings.tables.holidays,
    ]

    with sqlite3.connect(str(settings.sqlite_path)) as con:
        dfs = [
            read_table_as_df(con, t, from_date, to_date, parse_dates="datetime")
            for t in all_tables
        ]

    return pd.concat(dfs, axis=1, join="inner")

    # return
