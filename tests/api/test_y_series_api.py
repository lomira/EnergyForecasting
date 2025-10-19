import pandas as pd
from hypothesis import given
import pytest
from pydantic import ValidationError
from src.api.data.schemas.y_series_api import APITimeSeriesInput
from tests.strategies.timeseries_data_strategy import (
    TimeseriesStrategyPayload,
    create_TimeseriesStrategyPayload,
    timeseries_data_strategy,
    invalid_timeseries_data_strategy_missing_row,
    invalid_timeseries_data_strategy_duplicatetime,
    invalid_timeseries_data_strategy_wrong_timezone,
)


def test_missing_value_column() -> None:
    csv_text = """
        timestamp
        2023-01-01 00:00:00+00:00
        2023-01-02 00:00:00+00:00
        2023-01-03 00:00:00+00:00
        """
    payload = create_TimeseriesStrategyPayload(
        csv_text=csv_text,
        granularity="daily",
        freq_symbol="D",
        periods=3,
        timezone="UTC",
    )
    with pytest.raises(
        ValueError, match="CSV must contain exactly two columns: timestamp and value.*"
    ):
        raw_data = (payload["csv_text"], payload["granularity"], payload["timezone"])
        APITimeSeriesInput.from_api_data(raw_data)


def test_3_column_value_column() -> None:
    csv_text = """
        timestamp, value, invalid_col
        2023-01-01 00:00:00+00:00, 1, 2
        2023-01-02 00:00:00+00:00, 20, 3
        2023-01-03 00:00:00+00:00, 50, 4
        """
    payload = create_TimeseriesStrategyPayload(
        csv_text=csv_text,
        granularity="daily",
        freq_symbol="D",
        periods=3,
        timezone="UTC",
    )
    with pytest.raises(
        ValueError, match="CSV must contain exactly two columns: timestamp and value.*"
    ):
        raw_data = (payload["csv_text"], payload["granularity"], payload["timezone"])
        APITimeSeriesInput.from_api_data(raw_data)


def test_non_numeric_value_column() -> None:
    csv_text = """
        timestamp, value
        2023-01-01 00:00:00+00:00, abc
        2023-01-02 00:00:00+00:00, 20
        2023-01-03 00:00:00+00:00, 50
        """
    payload = create_TimeseriesStrategyPayload(
        csv_text=csv_text,
        granularity="daily",
        freq_symbol="D",
        periods=3,
        timezone="UTC",
    )
    with pytest.raises(ValidationError, match="Data column must be numeric.*"):
        raw_data = (payload["csv_text"], payload["granularity"], payload["timezone"])
        APITimeSeriesInput.from_api_data(raw_data)


def test_nan_in_value_column() -> None:
    csv_text = """
        timestamp, value
        2023-01-01 00:00:00+00:00, 10
        2023-01-02 00:00:00+00:00, NaN
        2023-01-03 00:00:00+00:00, 50
        """
    payload = create_TimeseriesStrategyPayload(
        csv_text=csv_text,
        granularity="daily",
        freq_symbol="D",
        periods=3,
        timezone="UTC",
    )
    with pytest.raises(ValidationError, match="Value error, Data column must be numeric.*"):
        raw_data = (payload["csv_text"], payload["granularity"], payload["timezone"])
        APITimeSeriesInput.from_api_data(raw_data)


def test_non_positive_value_column() -> None:
    csv_text = """
        timestamp, value
        2023-01-01 00:00:00+00:00, 10
        2023-01-02 00:00:00+00:00, -5
        2023-01-03 00:00:00+00:00, 50
        """
    payload = create_TimeseriesStrategyPayload(
        csv_text=csv_text,
        granularity="daily",
        freq_symbol="D",
        periods=3,
        timezone="UTC",
    )
    with pytest.raises(ValidationError, match="Data column contains non-positive values.*"):
        raw_data = (payload["csv_text"], payload["granularity"], payload["timezone"])
        APITimeSeriesInput.from_api_data(raw_data)


@given(ts_data_payload=timeseries_data_strategy())
def test_valid_data(ts_data_payload: TimeseriesStrategyPayload) -> None:
    csv_text = ts_data_payload["csv_text"]
    expected_granularity = ts_data_payload["granularity"]
    expected_timezone = ts_data_payload["timezone"]
    expected_periods = ts_data_payload["periods"]
    expected_freq_symbol = ts_data_payload["freq_symbol"]

    raw_data = (csv_text, expected_granularity, expected_timezone)
    api_input = APITimeSeriesInput.from_api_data(raw_data)

    # Basic type and field validation
    assert isinstance(api_input, APITimeSeriesInput), "Should create APITimeSeriesInput instance"
    assert api_input.granularity == expected_granularity, (
        f"Granularity mismatch: {api_input.granularity} != {expected_granularity}"
    )
    assert api_input.timezone == expected_timezone, (
        f"Timezone mismatch: {api_input.timezone} != {expected_timezone}"
    )

    # Frequency inference: pandas may return 'ME' for month-end; normalize to 'MS' for comparison
    inferred_freq = pd.infer_freq(api_input.dataframe.index).upper()
    if inferred_freq == "ME":
        inferred_freq = "MS"
    assert inferred_freq == expected_freq_symbol.upper(), (
        f"Frequency mismatch: {inferred_freq} != {expected_freq_symbol}"
    )

    # DataFrame structure and length validation
    assert isinstance(api_input.dataframe, pd.DataFrame), "DataFrame should be a pandas DataFrame"
    assert len(api_input.dataframe.index) == expected_periods, (
        f"Length mismatch: {len(api_input.dataframe.index)} != {expected_periods}"
    )
    assert str(api_input.dataframe.index.tz) == expected_timezone, (
        f"Index timezone mismatch: {api_input.dataframe.index.tz} != {expected_timezone}"
    )


@given(ts_data_payload=invalid_timeseries_data_strategy_missing_row())
def test_data_generation_with_missing_row(ts_data_payload: TimeseriesStrategyPayload) -> None:
    csv_text = ts_data_payload["csv_text"]
    nb_obs = ts_data_payload["periods"]
    expected_granularity = ts_data_payload["granularity"]
    expected_timezone = ts_data_payload["timezone"]

    raw_data = (csv_text, expected_granularity, expected_timezone)

    # If less than 3 obs then it should raaise a value error
    if nb_obs < 3:
        with pytest.raises(
            ValidationError, match="DataFrame must contain at least three data points."
        ):
            APITimeSeriesInput.from_api_data(raw_data)
    else:
        with pytest.raises(
            ValidationError,
            match="DataFrame index frequency could not be inferred; data may be irregular.",
        ):
            APITimeSeriesInput.from_api_data(raw_data)


@given(ts_data_payload=invalid_timeseries_data_strategy_duplicatetime())
def test_data_generation_with_duplicate_timestamp(
    ts_data_payload: TimeseriesStrategyPayload,
) -> None:
    csv_text = ts_data_payload["csv_text"]
    expected_granularity = ts_data_payload["granularity"]
    expected_timezone = ts_data_payload["timezone"]

    raw_data = (csv_text, expected_granularity, expected_timezone)

    with pytest.raises(
        ValidationError,
        match="DataFrame index frequency could not be inferred; data may be irregular.",
    ):
        APITimeSeriesInput.from_api_data(raw_data)


@given(ts_data_payload=invalid_timeseries_data_strategy_wrong_timezone())
def test_data_generation_with_wrong_timezone(
    ts_data_payload: TimeseriesStrategyPayload,
) -> None:
    csv_text = ts_data_payload["csv_text"]
    expected_granularity = ts_data_payload["granularity"]
    expected_timezone = ts_data_payload["timezone"]

    raw_data = (csv_text, expected_granularity, expected_timezone)

    with pytest.raises(
        ValueError,  # Value Error raised before Pydantic ValidationError
        match=f"Timezone '{expected_timezone}' not allowed.*",
    ):
        APITimeSeriesInput.from_api_data(raw_data)


def test_unsupported_granularity() -> None:
    csv_text = """
        timestamp, value
        2023-01-01 00:00:00+00:00, 10
        2023-01-02 00:00:00+00:00, 20
        2023-01-03 00:00:00+00:00, 30
        """
    granularity = "unsupported_granularity"
    timezone = "UTC"

    raw_data = (csv_text, granularity, timezone)

    with pytest.raises(
        ValueError,
        match=f"Granularity '{granularity}' is not supported.*",
    ):
        APITimeSeriesInput.from_api_data(raw_data)
