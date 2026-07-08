from contextlib import contextmanager

import duckdb
import pandas as pd


@contextmanager
def duckdb_conn(db_path: str, read_only: bool = False):
    config = {"access_mode": "read_only" if read_only else "read_write"}
    con = duckdb.connect(db_path, config=config)
    try:
        # Pass the connection to the 'with' block
        yield con
    finally:
        con.close()


def add_rows(con: duckdb.DuckDBPyConnection, table_name: str, df: pd.DataFrame) -> None:
    """Add rows to a DuckDB table."""
    con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
