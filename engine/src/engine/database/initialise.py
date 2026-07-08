from contextlib import contextmanager
from pathlib import Path

import duckdb
from engine.config import settings
from engine.data_model.load_model import LoadTimeSeries
from engine.data_model.type_mapping import TYPE_MAPPING
from pydantic import BaseModel


def create_table_from_model(
    con: duckdb.DuckDBPyConnection, model: type[BaseModel], table_name: str
):
    """Generates and executes a CREATE TABLE statement dynamically from a Pydantic model."""
    columns = []

    for field_name, field_info in model.model_fields.items():
        # Get the underlying type
        py_type = field_info.annotation
        db_type = TYPE_MAPPING.get(py_type, "VARCHAR")

        # Check if we sneaked a PRIMARY KEY hint into the description
        constraint = "PRIMARY KEY" if field_info.description == "PRIMARY KEY" else ""

        columns.append(f"{field_name} {db_type} {constraint}".strip())

    schema_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"
    con.execute(schema_sql)


def create_database(db_path: Path | None = None) -> Path:
    """Create a DuckDB database file and initialize the expected table."""
    database_path = Path(db_path or settings.duckdb_path).resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(database_path), read_only=False) as con:
        create_table_from_model(con, LoadTimeSeries, "load_time_series")

    return database_path


@contextmanager
def duckdb_conn(db_path: str, read_only: bool = False):
    config = {"read_only": "true" if read_only else "false"}
    con = duckdb.connect(db_path, config=config)
    try:
        if not read_only:
            create_table_from_model(con, LoadTimeSeries, "load_time_series")

        yield con
    finally:
        con.close()
