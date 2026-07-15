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
