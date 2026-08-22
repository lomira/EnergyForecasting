"""Build the source and derived datasets used by forecasting."""

from pathlib import Path
from typing import cast

import pandas as pd
from loguru import logger

import holiday_data
import load_data
import weather_data
from engine.features.calendar import weekday_features
from engine.features.holidays import holiday_features
from engine.features.weather import WEATHER_LOOKBACK, weather_features
from engine.storage.datasets import DB_PATH, replace_datasets

WORKSPACE_ROOT = Path(__file__).resolve().parents[4]


def initialize_source_databases() -> None:
    """Create source databases that do not exist yet."""
    if not load_data.DB_PATH.exists():
        load_data.import_excel(
            file_path=WORKSPACE_ROOT / "data" / "raw" / "excel" / "BDD_E.xlsx",
            sheet_name="Feuil1",
        )

    start_date, end_date = load_data.get_date_range()
    if not holiday_data.DB_PATH.exists():
        holiday_data.sync(start_date, end_date)
    if not weather_data.DB_PATH.exists():
        weather_data.sync(start_date - WEATHER_LOOKBACK, end_date)  # ty: ignore[invalid-argument-type]


def _validate_load_data(data: pd.DataFrame) -> None:
    if data.empty or data.index.isna().any() or data["load_mw"].isna().any():
        raise ValueError("Load data must not contain missing values")
    if data["load_mw"].lt(0).any():
        raise ValueError("Load values must be non-negative")
    if data.index.has_duplicates:
        raise ValueError("Load timestamps must not contain duplicates")
    if not data.index.equals(
        pd.date_range(data.index.min(), data.index.max(), freq="h")
    ):
        raise ValueError("Load data must be a continuous hourly series")


def _validate_covariates(data: pd.DataFrame) -> None:
    if data.empty or data.index.isna().any():
        raise ValueError("Covariates must not be empty or have missing timestamps")
    if data.index.has_duplicates:
        raise ValueError("Covariate timestamps must not contain duplicates")
    if not data.index.equals(
        pd.date_range(data.index.min(), data.index.max(), freq="h")
    ):
        raise ValueError("Covariates must be a continuous hourly series")
    missing_columns = data.columns[data.isna().any()].tolist()
    if missing_columns:
        raise ValueError(f"Covariates contain missing values: {missing_columns}")


def _future_covariates(from_date: pd.Timestamp, to_date: pd.Timestamp) -> pd.DataFrame:
    weather = weather_features(from_date, to_date)
    holidays = holiday_features(from_date, to_date)
    index = weather.index.union(holidays.index).sort_values()
    return pd.concat([weather, holidays, weekday_features(index)], axis=1, join="outer")


def populate_internal_db(*, db_path: Path = DB_PATH) -> None:
    """Validate and replace the datasets consumed by forecasting."""
    corrected_load = load_data.read()
    _validate_load_data(corrected_load)
    from_date = cast(pd.Timestamp, corrected_load.index.min())
    to_date = cast(pd.Timestamp, corrected_load.index.max())
    future_covariates = _future_covariates(from_date, to_date)
    _validate_covariates(future_covariates)
    replace_datasets(corrected_load, future_covariates, db_path=db_path)
    logger.info(
        f"Stored {len(corrected_load):,.0f} corrected loads and "
        f"{len(future_covariates):,.0f} future covariate rows"
    )
