import duckdb


def add_rows(con: duckdb.DuckDBPyConnection, table_name: str, df) -> None:
    """Add rows to a DuckDB table."""
    con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
