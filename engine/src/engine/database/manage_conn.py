from contextlib import contextmanager

import duckdb


@contextmanager
def duckdb_conn(db_path: str, read_only: bool = False):
    config = {"read_only": "true" if read_only else "false"}
    con = duckdb.connect(db_path, config=config)
    try:
        # Pass the connection to the 'with' block
        yield con
    finally:
        con.close()
