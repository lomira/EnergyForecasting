"""Derived SQLite data consumed by the forecasting engine."""

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import cast

import pandas as pd
from loguru import logger

import load_data
from engine.featurize.calendar import weekday_features
from engine.featurize.holidays import holiday_features
from engine.featurize.weather import weather_features

DB_PATH = load_data.DB_PATH.with_name("internal.sqlite3")


def _validate_load_data(data: pd.DataFrame) -> None:
    """Reject incomplete external load data before creating corrected data."""
    if data.empty or data.index.isna().any() or data["load_mw"].isna().any():
        raise ValueError("Load data must not contain missing values")
    if data["load_mw"].lt(0).any():
        raise ValueError("Load values must be non-negative")
    if data.index.has_duplicates:
        raise ValueError("Load timestamps must not contain duplicates")
    expected_index = pd.date_range(data.index.min(), data.index.max(), freq="h")
    if not data.index.equals(expected_index):
        raise ValueError("Load data must be a continuous hourly series")


def _validate_covariates(data: pd.DataFrame) -> None:
    """Reject incomplete covariates before storing them."""
    if data.empty or data.index.isna().any():
        raise ValueError("Covariates must not be empty or have missing timestamps")
    if data.index.has_duplicates:
        raise ValueError("Covariate timestamps must not contain duplicates")
    expected_index = pd.date_range(data.index.min(), data.index.max(), freq="h")
    if not data.index.equals(expected_index):
        raise ValueError("Covariates must be a continuous hourly series")
    missing_columns = data.columns[data.isna().any()].tolist()
    if missing_columns:
        raise ValueError(f"Covariates contain missing values: {missing_columns}")


def _future_covariates(from_date: pd.Timestamp, to_date: pd.Timestamp) -> pd.DataFrame:
    weather = weather_features(from_date, to_date)
    holidays = holiday_features(from_date, to_date)

    index = weather.index.union(holidays.index).sort_values()
    weekdays = weekday_features(index)
    return pd.concat([weather, holidays, weekdays], axis=1, join="outer")


def populate_internal_db(*, db_path: Path = DB_PATH) -> None:
    """Replace the corrected-load and future-covariate tables."""
    corrected_load = load_data.read()
    _validate_load_data(corrected_load)
    from_date = cast(pd.Timestamp, corrected_load.index.min())
    to_date = cast(pd.Timestamp, corrected_load.index.max())
    future_covariates = _future_covariates(from_date, to_date)
    _validate_covariates(future_covariates)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(db_path)) as connection, connection:
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
