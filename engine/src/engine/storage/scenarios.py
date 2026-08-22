"""Persistence for materialized hourly scenarios."""

import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd
from darts import TimeSeries

from engine.scenarios import validate_hourly_scenario
from engine.storage.datasets import DB_PATH
from engine.storage.sqlite import database


def _create_table(connection: sqlite3.Connection) -> None:
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
    with database(db_path) as connection:
        _create_table(connection)
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
    with database(db_path) as connection:
        _create_table(connection)
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


def delete_hourly_scenario(
    scenario_id: str,
    *,
    db_path: Path = DB_PATH,
) -> None:
    """Delete a materialized hourly scenario by ID."""
    with database(db_path) as connection:
        _create_table(connection)
        deleted = connection.execute(
            "DELETE FROM hourly_scenario WHERE scenario_id = ?", [scenario_id]
        ).rowcount
    if not deleted:
        raise ValueError(f"Unknown hourly scenario: {scenario_id}")
