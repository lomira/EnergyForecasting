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

        # First check if there is some timezone in the first element of timestamp column
        if pd.api.types.is_string_dtype(df["timestamp"]):
            first_timestamp = df["timestamp"].iloc[0]
            if pd.to_datetime(first_timestamp, errors="coerce").tzinfo is None:
                # Localize to the specified timezone if not tz is provided

                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="raise").dt.tz_localize(  # ty: ignore[unresolved-attribute]
                    raw_tz
                )
            else:
                # Convert to utc to ensure timezone consistency
                df["timestamp"] = pd.to_datetime(
                    df["timestamp"], errors="raise", utc=True
                ).dt.tz_convert(  # ty: ignore[unresolved-attribute]
                    raw_tz
                )
        df.set_index("timestamp", inplace=True)

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

        if len(df.index) < 3:
            raise ValueError("DataFrame must contain at least three data points.")

        # Check if the date index contains all dates from start to end with no gaps
        infer_freq = pd.infer_freq(df.index)
        if infer_freq is None:
            raise ValueError(
                "DataFrame index frequency could not be inferred; data may be irregular."
            )
        full_index = pd.date_range(
            start=df.index.min(), end=df.index.max(), freq=pd.infer_freq(df.index)
        )
        if not df.index.equals(full_index):
            raise ValueError("DataFrame index frequency could be inferred, but missing dates.")

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
