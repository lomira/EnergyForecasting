import io

import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator

from src.config import get_settings
from src.data.schemas.y_series import TimeSeriesData

SETTINGS = get_settings()
ALLOWED_TIMEZONES = SETTINGS.allowed_timezones
GRANULARITY_FREQ_MAP = SETTINGS.granularity_freq_map


class APITimeSeriesInput(BaseModel):
    """Represents raw API payload after initial parsing & normalization."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    dataframe: pd.DataFrame
    granularity: str
    timezone: str

    @classmethod
    def from_api_data(cls, raw_data: tuple) -> "APITimeSeriesInput":
        """Parse raw tuple (csv_text, granularity, timezone) into validated APITimeSeriesInput."""
        raw_df_text, raw_granularity, raw_tz = raw_data

        # Validate timezone early
        if raw_tz not in ALLOWED_TIMEZONES:
            raise ValueError(f"Timezone '{raw_tz}' not allowed. Allowed: {ALLOWED_TIMEZONES}")

        # Parse CSV text into DataFrame
        try:
            df = pd.read_csv(io.StringIO(str(raw_df_text).strip()))
        except Exception as e:
            raise ValueError("Failed to convert raw CSV text to DataFrame.") from e

        if "timestamp" not in df.columns:
            raise ValueError("CSV must contain a 'timestamp' column.")
        if len(df.columns) != 2:
            raise ValueError(
                f"CSV must contain exactly two columns: timestamp and value. \
                    Found: {list(df.columns)}"
            )

        try:
            # remove the time zone info from the timestamp column before localizing parsing the string
            tz_info_regex = r"([+-]\d{2}:?\d{2}|Z)$"
            timestamps_strings = df["timestamp"].astype(str)
            timestamps_no_tz = timestamps_strings.str.replace(tz_info_regex, "", regex=True)
            timestamps_parsed = pd.to_datetime(timestamps_no_tz, errors="raise")
            df["timestamp"] = pd.Series(timestamps_parsed).dt.tz_localize(raw_tz)
        except Exception as e:
            raise ValueError("Failed to parse 'timestamp' values.") from e
        # Set index
        df.set_index("timestamp", inplace=True)

        # Rename value column to a standard name if needed (keep first non-timestamp column)
        value_col = df.columns[0]
        if value_col.lower() != "value":
            df.rename(columns={value_col: "value"}, inplace=True)

        return cls(dataframe=df, granularity=raw_granularity, timezone=raw_tz)

    @field_validator("granularity")
    def validate_granularity(cls, v: str) -> str:
        if v not in GRANULARITY_FREQ_MAP:
            raise ValueError(
                f"Granularity '{v}' is not supported. \
                    Allowed: {list(GRANULARITY_FREQ_MAP.keys())}"
            )
        return v

    @field_validator("timezone")
    def validate_timezone(cls, v: str) -> str:
        if v not in ALLOWED_TIMEZONES:
            raise ValueError(f"Timezone '{v}' is not in allowed list: {ALLOWED_TIMEZONES}")
        return v

    @field_validator("dataframe")
    def validate_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be a DatetimeIndex.")
        if df.index.tz is None:
            raise ValueError("DataFrame index must be timezone-aware.")
        if len(df.columns) != 1:
            raise ValueError(
                f"DataFrame must have exactly one data column, found {len(df.columns)}."
            )
        value_series = df.iloc[:, 0]
        if not pd.api.types.is_numeric_dtype(value_series):
            raise ValueError("Data column must be numeric.")
        if value_series.isnull().any():
            raise ValueError("Data column contains null/NaN values.")
        if (value_series <= 0).any():
            raise ValueError("Data column contains non-positive values.")
        return df

    def to_timeseries(self) -> TimeSeriesData:
        """Convert validated API input into domain TimeSeriesData."""
        return TimeSeriesData(dataframe=self.dataframe, granularity=self.granularity)
