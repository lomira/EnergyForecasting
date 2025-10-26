import re
from pathlib import Path

import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator

from src.config import get_settings

SETTINGS = get_settings()
RAW_DATA_PATH = SETTINGS.data_dir / SETTINGS.raw_dir
ALLOWED_TIMEZONES = SETTINGS.allowed_timezones
GRANULARITY_FREQ_MAP = SETTINGS.granularity_freq_map


class TimeSeriesData(BaseModel):
    """Represents the structured, validated time series data."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    dataframe: pd.DataFrame
    granularity: str
    name: str

    @field_validator("name")
    def validate_name(cls, v: str, values) -> str:
        """Esnure name is not empty string"""
        if not v or v.strip() == "":
            raise ValueError("Name must be a non-empty string.")
        return v

    @field_validator("dataframe")
    def validate_dataframe_schema_content(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Validate that the dataframe has the required schema and content."""
        # 1. Validate the Index (Timestamp and Timezone)
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be a DatetimeIndex.")
        if df.index.tz is None:
            raise ValueError("DataFrame index must be timezone-aware.")

        # 2. Check that the timezone is in the allowed list
        tz_name = str(df.index.tz)
        if tz_name not in ALLOWED_TIMEZONES:
            raise ValueError(
                f"Timezone '{tz_name}' is not in the allowed list: {ALLOWED_TIMEZONES}"
            )

        # 3. Validate the Column Structure
        if len(df.columns) != 1:
            raise ValueError(
                f"DataFrame must have exactly one data column, {len(df.columns)} found"
            )

        # 4. Validate the Data Column Content (Vectorized Checks)
        value_series = df.iloc[:, 0]
        if not pd.api.types.is_numeric_dtype(value_series):
            raise ValueError("The data column must be a numeric type.")

        if value_series.isnull().any():
            raise ValueError("The data column contains null/NaN values.")

        return df

    @field_validator("granularity")
    def validate_granularity(cls, v: str) -> str:
        """Validate that the granularity is supported."""
        if v not in GRANULARITY_FREQ_MAP:
            raise ValueError(f"Granularity {v} is not supported.")
        return v

    @classmethod
    def from_csv(cls, file_path: Path) -> "TimeSeriesData":
        """Load time series data from a CSV file and create a TimeSeriesData instance"""

        # 0. Check if file exists and file extension is supported
        if not file_path.exists():
            raise FileNotFoundError(f"File {file_path} does not exist.")
        if file_path.suffix.lower() not in SETTINGS.supported_file_extensions:
            raise ValueError(
                f"File extension {file_path.suffix} is not supported. \
                    Supported extensions: {SETTINGS.supported_file_extensions}"
            )
        # Files are always named like YYY_XXXX_{granularity}.csv
        # 1. Extract granularity from filename
        match = re.search(r"_([^_]+)\.csv$", file_path.name, re.IGNORECASE)
        if not match:
            raise ValueError(f"Could not extract granularity from filename: {file_path.name}")
        granularity_str = match.group(1).lower()

        # 2. Read CSV and set the timestamp column as the index
        df = pd.read_csv(file_path, index_col="timestamp", parse_dates=True)

        return cls(
            dataframe=df,
            granularity=granularity_str,
        )

    # Write the dataframe to CSV
    def to_csv(self) -> str:
        """Write the time series DataFrame to a CSV file and return CSV text."""
        from src.config import get_settings

        settings = get_settings()
        file_path = settings.data_dir / settings.raw_dir / f"{self.name}_{self.granularity}.csv"
        # ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        # get CSV text
        csv_text = self.dataframe.to_csv()
        # write CSV text to disk
        file_path.write_text(csv_text, encoding="utf-8")
        return csv_text
