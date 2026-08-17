"""SQLite persistence for weather observations."""

import sqlite3
from collections.abc import Generator
from contextlib import closing, contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd

from weather_data.config import DB_PATH, WEATHER_API_PARAMS


@contextmanager
def _database(db_path: Path) -> Generator[sqlite3.Connection]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    metric_columns = ",\n".join(f'"{name}" REAL' for name in WEATHER_API_PARAMS)
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS weather_observation (
                datetime TEXT NOT NULL,
                city TEXT NOT NULL,
                {metric_columns},
                PRIMARY KEY (datetime, city)
            )
            """
        )
        yield connection
        connection.commit()


def _upsert(
    rows: dict[tuple[pd.Timestamp, str], dict[str, object]],
    *,
    db_path: Path = DB_PATH,
) -> None:
    columns = ("datetime", "city", *WEATHER_API_PARAMS)
    quoted_columns = ", ".join(f'"{name}"' for name in columns)
    updates = ", ".join(f'"{name}" = excluded."{name}"' for name in WEATHER_API_PARAMS)
    sql = (
        f"INSERT INTO weather_observation ({quoted_columns}) "
        f"VALUES ({', '.join('?' for _ in columns)}) "
        f"ON CONFLICT (datetime, city) DO UPDATE SET {updates}"
    )
    records = [
        (
            pd.Timestamp(row["datetime"]).isoformat(sep=" "),
            str(row["city"]),
            *(
                None if pd.isna(row.get(name)) else float(row[name])
                for name in WEATHER_API_PARAMS
            ),
        )
        for row in rows.values()
    ]
    with _database(db_path) as connection:
        connection.executemany(sql, records)


def read(
    from_date: datetime,
    to_date: datetime,
    *,
    db_path: Path = DB_PATH,
) -> pd.DataFrame:
    """Read weather observations pivoted to ``<city>_<metric>`` columns."""
    metric_columns = ", ".join(f'"{name}"' for name in WEATHER_API_PARAMS)
    with _database(db_path) as connection:
        data = pd.read_sql_query(
            f"""
            SELECT datetime, city, {metric_columns}
            FROM weather_observation
            WHERE datetime BETWEEN ? AND ?
            ORDER BY datetime, city
            """,
            connection,
            params=(
                pd.Timestamp(from_date).isoformat(sep=" "),
                pd.Timestamp(to_date).isoformat(sep=" "),
            ),
            parse_dates=["datetime"],
        )
    if data.empty:
        raise ValueError("No weather observations found in the requested range")
    tidy = data.pivot_table(
        index="datetime",
        columns="city",
        values=list(WEATHER_API_PARAMS),
        aggfunc="first",
    ).sort_index()
    tidy.columns = [f"{city}_{metric}" for metric, city in tidy.columns]
    return tidy
