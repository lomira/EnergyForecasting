"""Persistence for accepted backtest results."""

import json
import pickle
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

import pandas as pd
from darts import TimeSeries

from engine.forecasting.spec import BacktestSpec
from engine.storage.datasets import DB_PATH
from engine.storage.sqlite import database


@dataclass
class BacktestResult:
    backtest_id: str
    created_at: pd.Timestamp
    configuration_name: str
    model_class: str
    validation_start: pd.Timestamp
    validation_end: pd.Timestamp
    config: dict
    spec: BacktestSpec
    metrics: dict[str, float]
    curve: pd.DataFrame


def _create_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS backtest_result (
            backtest_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            configuration_name TEXT NOT NULL,
            model_class TEXT NOT NULL,
            validation_start TEXT NOT NULL,
            validation_end TEXT NOT NULL,
            config BLOB NOT NULL,
            spec BLOB NOT NULL,
            metrics_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS backtest_result_configuration_created
        ON backtest_result (configuration_name, created_at DESC);
        CREATE TABLE IF NOT EXISTS backtest_point (
            backtest_id TEXT NOT NULL,
            target_datetime TEXT NOT NULL,
            actual REAL NOT NULL,
            forecast REAL NOT NULL,
            PRIMARY KEY (backtest_id, target_datetime),
            FOREIGN KEY (backtest_id) REFERENCES backtest_result (backtest_id)
        );
        """
    )


def save_backtest_result(
    configuration_name: str,
    config: dict,
    spec: BacktestSpec,
    actual: TimeSeries,
    forecast: TimeSeries,
    *,
    metrics: dict[str, float],
    db_path: Path = DB_PATH,
) -> str:
    """Persist an accepted backtest and return its generated ID."""
    if not configuration_name.strip():
        raise ValueError("configuration_name must not be empty")
    if actual.n_components != 1 or forecast.n_components != 1:
        raise ValueError("Backtest actual and forecast series must be univariate")

    forecast_values = forecast.to_series()
    actual_values = actual.to_series().reindex(forecast_values.index)
    if forecast_values.empty or forecast_values.isna().any():
        raise ValueError("Backtest forecast must not be empty or contain null values")
    if actual_values.isna().any():
        raise ValueError("Actual load must cover every forecast timestamp")

    metrics_json = json.dumps(metrics, allow_nan=False)
    model_class = config["model_cls"].__name__
    backtest_id = str(uuid4())
    created_at = datetime.now(UTC).isoformat()

    with database(db_path) as connection:
        _create_tables(connection)
        connection.execute(
            """
            INSERT INTO backtest_result (
                backtest_id, created_at, configuration_name, model_class,
                validation_start, validation_end, config, spec, metrics_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                backtest_id,
                created_at,
                configuration_name,
                model_class,
                forecast_values.index.min().isoformat(sep=" "),
                forecast_values.index.max().isoformat(sep=" "),
                sqlite3.Binary(pickle.dumps(config)),
                sqlite3.Binary(pickle.dumps(spec)),
                metrics_json,
            ),
        )
        connection.executemany(
            """
            INSERT INTO backtest_point (
                backtest_id, target_datetime, actual, forecast
            ) VALUES (?, ?, ?, ?)
            """,
            [
                (
                    backtest_id,
                    timestamp.isoformat(sep=" "),
                    float(actual_values.loc[timestamp]),
                    float(forecast_values.loc[timestamp]),
                )
                for timestamp in forecast_values.index
            ],
        )
    return backtest_id


def list_backtest_results(
    configuration_name: str | None = None,
    *,
    db_path: Path = DB_PATH,
) -> pd.DataFrame:
    """List retained backtests, newest first."""
    where = " WHERE configuration_name = ?" if configuration_name is not None else ""
    params = [configuration_name] if configuration_name is not None else []
    with database(db_path) as connection:
        _create_tables(connection)
        data = pd.read_sql_query(
            """
            SELECT backtest_id, created_at, configuration_name, model_class,
                   validation_start, validation_end, metrics_json
            FROM backtest_result
            """
            + where
            + " ORDER BY created_at DESC",
            connection,
            params=params,
            parse_dates=["created_at", "validation_start", "validation_end"],
        )
    if data.empty:
        return data.drop(columns="metrics_json")
    metrics = pd.json_normalize(data.pop("metrics_json").map(json.loads))
    metrics.index = data.index
    return pd.concat([data, metrics], axis=1)


def read_backtest_result(
    backtest_id: str,
    *,
    db_path: Path = DB_PATH,
) -> BacktestResult:
    """Load a retained backtest by ID."""
    with database(db_path) as connection:
        _create_tables(connection)
        metadata = connection.execute(
            """
            SELECT created_at, configuration_name, model_class,
                   validation_start, validation_end, config, spec, metrics_json
            FROM backtest_result WHERE backtest_id = ?
            """,
            [backtest_id],
        ).fetchone()
        curve = pd.read_sql_query(
            """
            SELECT target_datetime, actual, forecast
            FROM backtest_point
            WHERE backtest_id = ? ORDER BY target_datetime
            """,
            connection,
            params=[backtest_id],
            parse_dates=["target_datetime"],
        )
    if metadata is None:
        raise ValueError(f"Unknown backtest result: {backtest_id}")
    curve = curve.set_index("target_datetime")
    curve.index.name = "datetime"
    return BacktestResult(
        backtest_id=backtest_id,
        created_at=cast(pd.Timestamp, pd.Timestamp(metadata[0])),
        configuration_name=metadata[1],
        model_class=metadata[2],
        validation_start=cast(pd.Timestamp, pd.Timestamp(metadata[3])),
        validation_end=cast(pd.Timestamp, pd.Timestamp(metadata[4])),
        config=pickle.loads(metadata[5]),
        spec=pickle.loads(metadata[6]),
        metrics=json.loads(metadata[7]),
        curve=curve,
    )
