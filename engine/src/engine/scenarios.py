"""Simple future-covariate scenario generation."""

from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd
from darts import TimeSeries

from engine.storage.datasets import read_future_covariates
from load_data import get_date_range


def validate_hourly_scenario(
    scenario: TimeSeries,
    references: Mapping[str, pd.DatetimeIndex],
) -> None:
    """Validate a materialized hourly scenario and its references."""
    data = scenario.to_dataframe()
    if not isinstance(data.index, pd.DatetimeIndex):
        raise TypeError("Scenario must use a DatetimeIndex")
    if set(data.columns) != set(references):
        raise ValueError("Scenario variables and reference variables must match")
    if data.isna().any().any():
        raise ValueError("Scenario values must not be null")
    expected_index = pd.date_range(data.index.min(), data.index.max(), freq="h")
    if not data.index.equals(expected_index):
        raise ValueError("Scenario must be a continuous hourly series")
    if any(
        len(timestamps) != len(data) or timestamps.isna().any()
        for timestamps in references.values()
    ):
        raise ValueError("Scenario references must match its length and not be null")


def create_hourly_scenario(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    references: Mapping[str, pd.DatetimeIndex],
) -> TimeSeries:
    """Create an inclusive hourly scenario from per-hour historical references."""
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")
    if start_date != start_date.floor("h") or end_date != end_date.floor("h"):
        raise ValueError("Scenario timestamps must be aligned to whole hours")
    if not references:
        raise ValueError("At least one scenario variable is required")

    target_index = pd.date_range(start_date, end_date, freq="h", name="datetime")
    if not all(
        isinstance(timestamps, pd.DatetimeIndex) for timestamps in references.values()
    ):
        raise TypeError("Scenario references must be pandas DatetimeIndex values")
    reference_indexes = dict(references)
    for variable, timestamps in reference_indexes.items():
        if len(timestamps) != len(target_index):
            raise ValueError(
                f"{variable} has {len(timestamps)} references; "
                f"expected {len(target_index)}"
            )

    all_timestamps = [
        timestamp
        for timestamps in reference_indexes.values()
        for timestamp in timestamps
    ]
    # Reads the full span; use point queries if this gets too costly.
    history = read_future_covariates(min(all_timestamps), max(all_timestamps))
    values: dict[str, object] = {}
    for variable, timestamps in reference_indexes.items():
        if variable not in history:
            raise ValueError(f"Unknown scenario variable: {variable}")
        missing = timestamps[~timestamps.isin(history.index)]
        if len(missing):
            raise ValueError(
                f"Missing reference timestamps for {variable}: {list(missing)}"
            )
        selected = history.loc[timestamps, variable]
        if selected.isna().any():
            raise ValueError(f"Null reference values for {variable}")
        values[variable] = selected.to_numpy()

    scenario = TimeSeries.from_dataframe(pd.DataFrame(values, index=target_index))
    validate_hourly_scenario(scenario, reference_indexes)
    return scenario


def random_future_scenario(
    variables: Sequence[str] | None,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> TimeSeries | None:
    """Create a scenario from a random contiguous historical period."""
    if not variables:
        return None
    scenario_length = len(pd.date_range(start_date, end_date, freq="h"))
    history_start, history_end = get_date_range()
    latest_start = history_end - pd.Timedelta(hours=scenario_length - 1)
    reference_start = pd.Timestamp(
        np.random.default_rng().choice(
            pd.date_range(history_start, latest_start, freq="h")
        )
    )
    reference_timestamps = pd.date_range(
        reference_start, periods=scenario_length, freq="h"
    )
    return create_hourly_scenario(
        start_date,
        end_date,
        {variable: reference_timestamps for variable in variables},
    )
