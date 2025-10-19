import pandas as pd
from typing import TypedDict
from hypothesis import strategies
from src.config import get_settings

SETTINGS = get_settings()


class TimeseriesStrategyPayload(TypedDict):
    """TypedDict for the timeseries test data strategy result."""

    csv_text: str
    granularity: str
    timezone: str
    periods: int
    freq_symbol: str


# Timezone strategy
timezone_strategy_in = strategies.sampled_from(["UTC", "Europe/Paris", None])
timezone_strategy_out = strategies.sampled_from(SETTINGS.allowed_timezones)

# Periods strategy
periods_strategy = strategies.integers(min_value=3, max_value=100)

# Frequency strategy
freq_map = SETTINGS.granularity_freq_map  # maps granularity -> freq symbol
# Invert mapping to derive granularity from freq symbol
granularity_from_freq = {v: k for k, v in freq_map.items()}
frequency_strategy = strategies.sampled_from(list(freq_map.values()))

# Timestamp index strategy
naive_start_date_strategy = strategies.datetimes(
    min_value=pd.Timestamp("1998-01-01").to_pydatetime(),
    max_value=pd.Timestamp("2025-12-31").to_pydatetime(),
)

# Value column strategy
value_element_strategy = strategies.floats(
    min_value=0.1, max_value=100_000.0, allow_nan=False, allow_infinity=False
)


@strategies.composite
def timeseries_data_strategy(draw) -> TimeseriesStrategyPayload:
    """Generate a realistic timeseries payload for testing API input."""
    # Draw basic parameters
    periods = draw(periods_strategy)
    freq_symbol = draw(frequency_strategy)
    timezone_in = draw(timezone_strategy_in)
    # if we draw No TZ as input then we must draw a valid one for output
    timezone_out = timezone_in if timezone_in is not None else draw(timezone_strategy_out)

    start_naive = pd.Timestamp(draw(naive_start_date_strategy)).tz_localize(None)
    # Floor the start time according to frequency to ensure clean, aligned series
    if freq_symbol == "h":
        floored_start_naive = start_naive.replace(minute=0, second=0, microsecond=0)
    elif freq_symbol == "d":
        floored_start_naive = start_naive.replace(hour=0, minute=0, second=0, microsecond=0)
    else:  # 'MS' for month start
        floored_start_naive = start_naive.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    floored_start_tz = floored_start_naive.tz_localize(timezone_out)

    # Generate random values for the series
    values_list = draw(strategies.lists(value_element_strategy, min_size=periods, max_size=periods))

    # Create a DatetimeIndex and DataFrame with the generated data
    ts_index = pd.date_range(start=floored_start_tz, periods=periods, freq=freq_symbol)
    df = pd.DataFrame({"value": values_list}, index=ts_index)

    # Convert DataFrame to CSV format expected by the API (timestamp column + value column)
    df_out = df.reset_index().rename(columns={"index": "timestamp"})
    csv_text = df_out.to_csv(index=False)

    # Derive granularity from frequency symbol
    granularity = granularity_from_freq.get(freq_symbol, "monthly")
    if freq_symbol == "MS":  # Special case as pandas may infer 'ME' for month end
        granularity = "monthly"

    return TimeseriesStrategyPayload(
        csv_text=csv_text,
        granularity=granularity,
        timezone=timezone_out,
        periods=periods,
        freq_symbol=freq_symbol,
    )
