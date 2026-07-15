import sqlite3
from datetime import datetime
from pathlib import Path
from typing import get_args

import pandera as pa
from engine.config.config import settings
from engine.data_model.load_model import LoadSchema
from engine.data_model.weather_model import build_weather_schema


def create_table_from_model(
    con: sqlite3.Connection, model: type[pa.SchemaModel], table_name: str
):
    """Generates and executes a CREATE TABLE statement dynamically from a Pandera SchemaModel."""

    TYPE_MAPPING = {
        int: "INTEGER",
        str: "TEXT",
        datetime: "TEXT",
        float: "REAL",
        bool: "INTEGER",
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
    """Create a SQLite database file and initialize the expected tables."""
    database_path = settings.sqlite_path.resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(str(database_path)) as con:
        create_table_from_model(con, LoadSchema, "load_time_series")
        weather_schema = build_weather_schema(
            weather_metrics=settings.weather_metrics,
            previous_days=settings.weather_metrics_previous_days,
        )
        create_table_from_model(con, weather_schema, "weather")

    return database_path
