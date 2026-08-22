"""Persistence for datasets consumed by the forecasting engine."""

from pathlib import Path

import pandas as pd
from loguru import logger

from engine.storage.sqlite import database

DB_PATH = Path(__file__).resolve().parents[4] / "db" / "internal.sqlite3"


def replace_datasets(
    corrected_load: pd.DataFrame,
    future_covariates: pd.DataFrame,
    *,
    db_path: Path = DB_PATH,
) -> None:
    """Replace the corrected-load and future-covariate tables."""
    with database(db_path) as connection:
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


def correct_loads_at(data: pd.DataFrame, *, db_path: Path = DB_PATH) -> None:
    """Update corrected loads from a dataframe with datetime and load_mw columns."""
    if set(data.columns) != {"datetime", "load_mw"}:
        raise ValueError("Corrections must have datetime and load_mw columns")

    corrections = data.copy()
    corrections["datetime"] = pd.to_datetime(corrections["datetime"], errors="coerce")
    corrections["load_mw"] = pd.to_numeric(corrections["load_mw"], errors="coerce")
    if corrections.isna().any().any():
        raise ValueError("Corrections must not contain missing or invalid values")
    if corrections["load_mw"].lt(0).any():
        raise ValueError("Load values must be non-negative")
    if corrections["datetime"].duplicated().any():
        raise ValueError("Correction timestamps must not contain duplicates")
    if not db_path.exists():
        raise ValueError(f"Internal database does not exist: {db_path}")

    with database(db_path) as connection:
        for timestamp, load_mw in corrections.itertuples(index=False, name=None):
            updated = connection.execute(
                "UPDATE corrected_load SET load_mw = ? WHERE datetime = ?",
                (float(load_mw), timestamp.isoformat(sep=" ")),
            )
            if updated.rowcount != 1:
                raise ValueError(f"No corrected load found at {timestamp}")
    logger.info(f"Corrected {len(corrections):,.0f} loads")


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
    with database(db_path) as connection:
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
