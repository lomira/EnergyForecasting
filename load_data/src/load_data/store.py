"""SQLite persistence for hourly load observations."""

import sqlite3
from collections.abc import Generator
from contextlib import closing, contextmanager
from pathlib import Path

import pandas as pd

DB_PATH = Path(__file__).resolve().parents[3] / "db" / "load.sqlite3"


@contextmanager
def _database(db_path: Path) -> Generator[sqlite3.Connection]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS load_observation (
                datetime TEXT PRIMARY KEY,
                load_mw REAL NOT NULL CHECK (load_mw >= 0)
            )
            """
        )
        yield connection
        connection.commit()


def _upsert(records: list[tuple[str, float]], *, db_path: Path = DB_PATH) -> None:
    with _database(db_path) as connection:
        connection.executemany(
            """
            INSERT INTO load_observation (datetime, load_mw) VALUES (?, ?)
            ON CONFLICT (datetime) DO UPDATE SET load_mw = excluded.load_mw
            """,
            records,
        )


def get_date_range(*, db_path: Path = DB_PATH) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return the first and last stored timestamps."""
    with _database(db_path) as connection:
        row = connection.execute(
            "SELECT MIN(datetime), MAX(datetime) FROM load_observation"
        ).fetchone()
    if row is None or row[0] is None or row[1] is None:
        raise ValueError("No load observations found in the database")
    return pd.Timestamp(row[0]), pd.Timestamp(row[1])


def read(
    from_date: pd.Timestamp | None = None,
    to_date: pd.Timestamp | None = None,
    *,
    db_path: Path = DB_PATH,
) -> pd.DataFrame:
    """Read load observations as a datetime-indexed dataframe."""
    clauses: list[str] = []
    params: list[str] = []
    if from_date is not None:
        clauses.append("datetime >= ?")
        params.append(pd.Timestamp(from_date).isoformat(sep=" "))
    if to_date is not None:
        clauses.append("datetime <= ?")
        params.append(pd.Timestamp(to_date).isoformat(sep=" "))
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""

    with _database(db_path) as connection:
        data = pd.read_sql_query(
            f"SELECT datetime, load_mw FROM load_observation{where} ORDER BY datetime",
            connection,
            params=params,
            parse_dates=["datetime"],
        )
    if data.empty:
        raise ValueError("No load observations found in the requested range")
    return data.set_index("datetime")
