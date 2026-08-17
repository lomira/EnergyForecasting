"""SQLite persistence for hourly holiday flags."""

import sqlite3
from collections.abc import Generator
from contextlib import closing, contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd

DB_PATH = Path(__file__).resolve().parents[3] / "db" / "holiday.sqlite3"


@contextmanager
def _database(db_path: Path) -> Generator[sqlite3.Connection]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS holiday_observation (
                datetime TEXT PRIMARY KEY,
                is_holiday INTEGER NOT NULL CHECK (is_holiday IN (0, 1))
            )
            """
        )
        yield connection
        connection.commit()


def _upsert(records: list[tuple[str, int]], *, db_path: Path = DB_PATH) -> None:
    with _database(db_path) as connection:
        connection.executemany(
            """
            INSERT INTO holiday_observation (datetime, is_holiday) VALUES (?, ?)
            ON CONFLICT (datetime) DO UPDATE SET is_holiday = excluded.is_holiday
            """,
            records,
        )


def read(
    from_date: datetime,
    to_date: datetime,
    *,
    db_path: Path = DB_PATH,
) -> pd.DataFrame:
    """Read holiday flags as a datetime-indexed dataframe."""
    with _database(db_path) as connection:
        data = pd.read_sql_query(
            """
            SELECT datetime, is_holiday AS holidays
            FROM holiday_observation
            WHERE datetime BETWEEN ? AND ?
            ORDER BY datetime
            """,
            connection,
            params=(
                pd.Timestamp(from_date).isoformat(sep=" "),
                pd.Timestamp(to_date).isoformat(sep=" "),
            ),
            parse_dates=["datetime"],
        )
    if data.empty:
        raise ValueError("No holiday observations found in the requested range")
    data["holidays"] = data["holidays"].astype(bool)
    return data.set_index("datetime")
