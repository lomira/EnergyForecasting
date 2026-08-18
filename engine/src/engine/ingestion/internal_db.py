"""Derived SQLite data consumed by the forecasting engine."""

import sqlite3
from pathlib import Path
from typing import cast

import pandas as pd
from darts import TimeSeries
from loguru import logger

import holiday_data
import load_data
import weather_data
from engine.featurize.calendar import encode_onehot_custom_weekday
from engine.featurize.features import Feature
from engine.featurize.lags import RollingLagTransformer

DB_PATH = load_data.DB_PATH.with_name("internal.sqlite3")


def _future_covariates(from_date: pd.Timestamp, to_date: pd.Timestamp) -> pd.DataFrame:
    weather = weather_data.read(from_date, to_date)
    weather = cast(
        TimeSeries,
        RollingLagTransformer().transform(TimeSeries.from_dataframe(weather)),
    ).to_dataframe()
    holidays = holiday_data.read(from_date, to_date)
    # Convert the daily holiday flags to hourly with forward fill
    holidays = pd.DataFrame(
        holidays.reindex(
            pd.date_range(
                start=from_date.floor(
                    "d"
                ),  # Ensuring that we get a match with the hourly data, we floor the from_date to the start of the day
                end=to_date,
                freq="h",
            ),
            method="ffill",
        ).fillna(0),
        columns=[Feature.HOLIDAYS.value],
    )
    # we remove the eventuals elements introduce by the floor date
    holidays = holidays[holidays.index >= from_date]

    index = weather.index.union(holidays.index).sort_values()
    weekdays = pd.DataFrame(
        encode_onehot_custom_weekday(index),
        index=index,
        columns=[f"custom_weekday_{day}" for day in range(1, 5)],
    )
    return pd.concat([weather, holidays, weekdays], axis=1, join="outer")


def populate_internal_db(*, db_path: Path = DB_PATH) -> None:
    """Replace the corrected-load and future-covariate tables."""
    corrected_load = load_data.read()
    from_date = cast(pd.Timestamp, corrected_load.index.min())
    to_date = cast(pd.Timestamp, corrected_load.index.max())
    future_covariates = _future_covariates(from_date, to_date)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as connection:
        corrected_load.to_sql(
            "corrected_load",
            connection,
            if_exists="replace",
            index=True,
            index_label="datetime",
        )
        connection.execute(
            "CREATE UNIQUE INDEX corrected_load_datetime ON corrected_load (datetime)"
        )
        future_covariates.to_sql(
            "future_covariates",
            connection,
            if_exists="replace",
            index=True,
            index_label="datetime",
        )
        connection.execute(
            "CREATE UNIQUE INDEX future_covariates_datetime "
            "ON future_covariates (datetime)"
        )
    logger.info(
        f"Stored {len(corrected_load):,.0f} corrected loads and "
        f"{len(future_covariates):,.0f} future covariate rows"
    )


def _read(
    table: str,
    from_date: pd.Timestamp | None,
    to_date: pd.Timestamp | None,
    db_path: Path,
) -> pd.DataFrame:
    if not db_path.exists():
        raise ValueError(f"Internal database does not exist: {db_path}")
    clauses: list[str] = []
    params: list[str] = []
    if from_date is not None:
        clauses.append("datetime >= ?")
        params.append(pd.Timestamp(from_date).isoformat(sep=" "))
    if to_date is not None:
        clauses.append("datetime <= ?")
        params.append(pd.Timestamp(to_date).isoformat(sep=" "))
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    with sqlite3.connect(db_path) as connection:
        data = pd.read_sql_query(
            f'SELECT * FROM "{table}"{where} ORDER BY datetime',
            connection,
            params=params,
            parse_dates=["datetime"],
        )
    if data.empty:
        raise ValueError(f"No {table.replace('_', ' ')} found in the requested range")
    return data.set_index("datetime")


def read_corrected_load(
    from_date: pd.Timestamp | None = None,
    to_date: pd.Timestamp | None = None,
    *,
    db_path: Path = DB_PATH,
) -> pd.DataFrame:
    return _read("corrected_load", from_date, to_date, db_path)


def read_future_covariates(
    from_date: pd.Timestamp,
    to_date: pd.Timestamp,
    *,
    db_path: Path = DB_PATH,
) -> pd.DataFrame:
    return _read("future_covariates", from_date, to_date, db_path)
