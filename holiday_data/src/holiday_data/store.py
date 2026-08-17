"""SQLite persistence for daily holiday flags."""

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
                date TEXT PRIMARY KEY,
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
            INSERT INTO holiday_observation (date, is_holiday) VALUES (?, ?)
            ON CONFLICT (date) DO UPDATE SET is_holiday = excluded.is_holiday
            """,
            records,
        )


def read(
    from_date: datetime,
    to_date: datetime,
    *,
    db_path: Path = DB_PATH,
) -> pd.DataFrame:
    """Read daily holiday flags expanded to an hourly dataframe."""
    with _database(db_path) as connection:
        data = pd.read_sql_query(
            """
            SELECT date, is_holiday AS holidays
            FROM holiday_observation
            WHERE date BETWEEN ? AND ?
            ORDER BY date
            """,
            connection,
            params=(
                pd.Timestamp(from_date).date().isoformat(),
                pd.Timestamp(to_date).date().isoformat(),
            ),
            parse_dates=["date"],
        )
    if data.empty:
        raise ValueError("No holiday observations found in the requested range")
    hours = pd.date_range(from_date, to_date, freq="h")
    data = data.set_index("date").reindex(hours.normalize()).set_axis(hours)
    data.index.name = "datetime"
    return data.dropna().astype(bool)
