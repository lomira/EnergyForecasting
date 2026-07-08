from datetime import datetime
from pathlib import Path
from typing import get_args

import duckdb
import pandera as pa
from engine.config import settings
from engine.data_model.load_model import LoadSchema


def create_table_from_model(
    con: duckdb.DuckDBPyConnection, model: type[pa.SchemaModel], table_name: str
):
    """Generates and executes a CREATE TABLE statement dynamically from a Pandera SchemaModel."""

    TYPE_MAPPING = {
        int: "INTEGER",
        str: "VARCHAR",
        datetime: "TIMESTAMP",
        float: "DOUBLE",
        bool: "BOOLEAN",
    }

    columns = []

    for field_name, field_type in model.__annotations__.items():
        # Extract the inner type from Series[...]
        # e.g., Series[pd.Timestamp] -> pd.Timestamp, Series[float] -> float
        args = get_args(field_type)
        py_type = args[0] if args else field_type

        db_type = TYPE_MAPPING.get(py_type, "VARCHAR")
        columns.append(f"{field_name} {db_type}".strip())

    schema_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"
    con.execute(schema_sql)


def create_database() -> Path:
    """Create a DuckDB database file and initialize the expected table."""
    database_path = settings.duckdb_path.resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(database_path), read_only=False) as con:
        create_table_from_model(con, LoadSchema, "load_time_series")

    return database_path
