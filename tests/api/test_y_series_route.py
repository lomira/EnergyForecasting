import pandas as pd
from hypothesis import given
from src.api.data.schemas.y_series_api import APITimeSeriesInput
from tests.strategies.timeseries_data_strategy import (
    timeseries_data_strategy,
    TimeseriesStrategyPayload,
)


@given(ts_data_payload=timeseries_data_strategy())
def test_data_generation_is_continuous(ts_data_payload: TimeseriesStrategyPayload) -> None:
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
