import pandas as pd
from hypothesis import given, strategies
from src.api.data.schemas.y_series_api import APITimeSeriesInput

from src.config import get_settings

SETTTINGS = get_settings()

# TODO Timezone handling in tests
print(f"Allowed timezones: {SETTTINGS.allowed_timezones}")
# STRATEGIES
# Timezone strategy
timezone_strategy = strategies.sampled_from(SETTTINGS.allowed_timezones)

# Periods strategy
periods_strategy = strategies.integers(min_value=3, max_value=100)

# Frequency strategy
Frequency = SETTTINGS.granularity_freq_map
frequency_strategy = strategies.sampled_from(list(Frequency.values()))

# Timestamp index strategy
naive_start_date_strategy = strategies.datetimes(
    min_value=pd.Timestamp("1998-01-01").to_pydatetime(),
    max_value=pd.Timestamp("2025-12-31").to_pydatetime(),
)

# Value column strategy
value_element_strategy = strategies.floats(
    min_value=0.1, max_value=100_000.0, allow_nan=False, allow_infinity=False
)


# Combining the two into a DatetimeIndex strategy
@strategies.composite
def timeseries_data_strategy(draw):
    periods = draw(periods_strategy)
    freq = draw(frequency_strategy)
    start_naive = draw(naive_start_date_strategy)
    timezone = draw(timezone_strategy)
    start_naive = pd.Timestamp(start_naive).tz_localize(timezone)
    # Instead of hardcoding freq mapping, we can reverse lookup from the config
    if freq == "h":
        freq_out = "hourly"
        floored_start_naive = pd.Timestamp(start_naive).replace(minute=0, second=0)
    elif freq == "d":
        freq_out = "daily"
        floored_start_naive = pd.Timestamp(start_naive).replace(hour=0, minute=0, second=0)
    else:
        freq_out = "monthly"
        floored_start_naive = pd.Timestamp(start_naive).replace(day=1, hour=0, minute=0, second=0)

    values_list = draw(strategies.lists(value_element_strategy, min_size=periods, max_size=periods))

    ts_index = pd.date_range(start=floored_start_naive, periods=periods, freq=freq)

    df = pd.DataFrame({"value": values_list}, index=ts_index)
    # Convert df to a simple dataframe where the index becomes a column named 'timestamp'
    df_out = df.reset_index().rename(columns={"index": "timestamp"})
    # and the value is a column named 'value'
    # convert the output to csv
    df_out_csv = df_out.to_csv(index=False)

    return df_out_csv, freq_out, timezone


@given(ts_data_tuple=timeseries_data_strategy())
def test_data_generation_is_continuous(ts_data_tuple: tuple[pd.DataFrame, str, str]) -> None:
    ts_data, granularity, timezone = ts_data_tuple
    raw_data = (ts_data, granularity, timezone)
    api_input = APITimeSeriesInput.from_api_data(raw_data)
    assert isinstance(api_input, APITimeSeriesInput)
