"""Persistence for materialized hourly scenarios."""

import sqlite3
from collections.abc import Generator, Mapping
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd
from darts import TimeSeries

from engine.ingestion.internal_db import DB_PATH
from engine.scenario.future_scenario import validate_hourly_scenario


@contextmanager
def _database(db_path: Path) -> Generator[sqlite3.Connection]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS hourly_scenario (
                scenario_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                target_datetime TEXT NOT NULL,
                variable_name TEXT NOT NULL,
                value REAL NOT NULL,
                reference_datetime TEXT NOT NULL,
                PRIMARY KEY (scenario_id, target_datetime, variable_name)
            )
            """
        )
        yield connection
        connection.commit()


def save_hourly_scenario(
    scenario: TimeSeries,
    references: Mapping[str, pd.DatetimeIndex],
    *,
    db_path: Path = DB_PATH,
) -> str:
    """Persist a materialized hourly scenario and return its generated ID."""
    validate_hourly_scenario(scenario, references)
    data = scenario.to_dataframe()
    scenario_id = str(uuid4())
    created_at = datetime.now(UTC).isoformat()
    records = [
        (
            scenario_id,
            created_at,
            target.isoformat(sep=" "),
            variable,
            float(data.at[target, variable]),
            references[variable][position].isoformat(sep=" "),
        )
        for position, target in enumerate(data.index)
        for variable in data.columns
    ]
    with _database(db_path) as connection:
        connection.executemany(
            """
            INSERT INTO hourly_scenario (
                scenario_id, created_at, target_datetime,
                variable_name, value, reference_datetime
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            records,
        )
    return scenario_id


def read_hourly_scenario(
    scenario_id: str,
    *,
    db_path: Path = DB_PATH,
) -> TimeSeries:
    """Load a materialized hourly scenario by ID."""
    with _database(db_path) as connection:
        data = pd.read_sql_query(
            """
            SELECT target_datetime, variable_name, value
            FROM hourly_scenario
            WHERE scenario_id = ?
            ORDER BY target_datetime, variable_name
            """,
            connection,
            params=[scenario_id],
            parse_dates=["target_datetime"],
        )
    if data.empty:
        raise ValueError(f"Unknown hourly scenario: {scenario_id}")
    frame = data.pivot(index="target_datetime", columns="variable_name", values="value")
    frame.index.name = "datetime"
    frame.columns.name = None
    return TimeSeries.from_dataframe(frame, fill_missing_dates=True, freq="h")
