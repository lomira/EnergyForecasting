import sqlite3
from datetime import datetime

import pandas as pd


def add_rows(con: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> None:
    """Add rows to a SQLite table."""
    desc = con.execute(f"PRAGMA table_info({table_name})").fetchall()
    col_order = [col[1] for col in desc if col[1] is not None]
    missing = [c for c in col_order if c not in df.columns]

    if missing:
        raise ValueError(
            f"DataFrame is missing required columns for table '{table_name}': {missing}"
        )

    df = df.reindex(columns=col_order)
    df.to_sql(table_name, con, if_exists="append", index=False)


def read_table_as_df(
    con: sqlite3.Connection,
    table_name: str,
    start: datetime | None = None,
    end: datetime | None = None,
    parse_dates: str = "datetime",
) -> pd.DataFrame:
    """Return a table from the database as a pandas dataframe"""
    start, end = start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")
    query = f"SELECT * FROM {table_name} WHERE datetime BETWEEN ? AND ?"
    df = pd.read_sql(query, con, params=(start, end), parse_dates=[parse_dates])
    return df.set_index(parse_dates)


def get_start_end_dates(con: sqlite3.Connection, table_name: str) -> tuple:
    """Get the start and end dates from a SQLite table."""
    query = f"SELECT MIN(datetime), MAX(datetime) FROM {table_name}"
    result = con.execute(query).fetchone()
    return result[0], result[1]
