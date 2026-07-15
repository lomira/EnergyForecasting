import sqlite3

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


def get_start_end_dates(con: sqlite3.Connection, table_name: str) -> tuple:
    """Get the start and end dates from a SQLite table."""
    query = f"SELECT MIN(datetime), MAX(datetime) FROM {table_name}"
    result = con.execute(query).fetchone()
    return result[0], result[1]


def get_all_covariates(
    con: sqlite3.Connection, start_date: pd.Timestamp, end_date: pd.Timestamp
) -> pd.DataFrame:
    """Get all covariates from the database within a specified date range."""
    list_covariates = [
        "weather_time_series",
        "holidays_time_series",
    ]
    query = """
        SELECT *
        FROM (
            SELECT * FROM weather_time_series
            UNION ALL
            SELECT * FROM holidays_time_series
        ) AS combined
        WHERE datetime BETWEEN ? AND ?
        ORDER BY datetime
    """
    df = pd.read_sql_query(query, con, params=(start_date, end_date))
    return df
