import duckdb


def add_rows(con: duckdb.DuckDBPyConnection, table_name: str, df) -> None:
    """Add rows to a DuckDB table."""
    desc = con.execute(f"DESCRIBE {table_name}").fetchall()
    col_order = [col[0] for col in desc if col[0] is not None]
    missing = [c for c in col_order if c not in df.columns]

    if missing:
        raise ValueError(
            f"DataFrame is missing required columns for table '{table_name}': {missing}"
        )

    # Reorder DataFrame columns to match table column order
    df = df.reindex(columns=col_order)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
