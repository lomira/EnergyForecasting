from typing import Union, Optional, Set
from datetime import date
from pydantic import BaseModel, field_validator, ConfigDict, Field, model_validator
from src.config import get_settings
import pandas as pd
from openmeteo_sdk.WeatherApiResponse import WeatherApiResponse


class OpenMeteoRequest(BaseModel):
    """Represents raw API payload for OpenMeteo API."""

    model_config = ConfigDict(extra="forbid")

    latitude: float = Field(..., ge=-90.0, le=90.0, description="WGS84 latitude")
    longitude: float = Field(..., ge=-180.0, le=180.0, description="WGS84 longitude")
    start_date: date
    end_date: date
    timezone: Optional[str] = "UTC"

    # Allows a single string: "temperature_2m"
    # Or a set of strings: ("temperature_2m", "relative_humidity_2m")
    hourly: Union[str, Set[str]]

    @field_validator("hourly")
    def validate_hourly_params(cls, v: Union[str, Set[str]]) -> set[str]:
        """Validate that the hourly parameters are supported."""
        params_list = set(v) if isinstance(v, str) else v
        allowed_params = get_settings().weather_variables_options
        for param in params_list:
            if param not in allowed_params:
                raise ValueError(
                    f"Hourly parameter '{param}' is not supported. Allowed parameters: {allowed_params}"
                )
        return params_list

    @model_validator(mode="after")
    def check_dates(self) -> "OpenMeteoRequest":
        """Validate that end_date is not earlier than start_date."""
        if self.start_date and self.end_date:
            if self.end_date < self.start_date:
                raise ValueError("end_date cannot be earlier than start_date")
        return self


# https://github.com/open-meteo/sdk/blob/main/python/openmeteo_sdk/Unit.py
UNIT_VALUE_TO_NAME = {
    0: "undefined",
    1: "celsius",
    2: "centimetre",
    3: "cubic_metre_per_cubic_metre",
    4: "cubic_metre_per_second",
    5: "degree_direction",
    6: "dimensionless_integer",
    7: "dimensionless",
    8: "european_air_quality_index",
    9: "fahrenheit",
    10: "feet",
    11: "fraction",
    12: "gdd_celsius",
    13: "geopotential_metre",
    14: "grains_per_cubic_metre",
    15: "gram_per_kilogram",
    16: "hectopascal",
    17: "hours",
    18: "inch",
    19: "iso8601",
    20: "joule_per_kilogram",
    21: "kelvin",
    22: "kilopascal",
    23: "kilogram_per_square_metre",
    24: "kilometres_per_hour",
    25: "knots",
    26: "megajoule_per_square_metre",
    27: "metre_per_second_not_unit_converted",
    28: "metre_per_second",
    29: "metre",
    30: "micrograms_per_cubic_metre",
    31: "miles_per_hour",
    32: "millimetre",
    33: "pascal",
    34: "per_second",
    35: "percentage",
    36: "seconds",
    37: "unix_time",
    38: "us_air_quality_index",
    39: "watt_per_square_metre",
    40: "wmo_code",
    41: "parts_per_million",
    42: "kilogram_per_cubic_metre",
}


class OpenMeteoResponse(BaseModel):
    """Represents the OpenMeteo API response schema."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    latitude: float = Field(..., ge=-90.0, le=90.0, description="WGS84 latitude")
    longitude: float = Field(..., ge=-180.0, le=180.0, description="WGS84 longitude")
    generationtime_ms: float = Field(
        description="Time taken to generate the response in milliseconds."
    )
    utc_offset_seconds: int = Field(description="The timezone offset in seconds.")
    elevation: float = Field(description="Elevation above sea level in meters.")
    data: pd.DataFrame = Field(
        description="Time-series weather data for all requested hourly variables."
    )
    units: dict = Field(description="Units for each variable in the data DataFrame.")

    @field_validator("utc_offset_seconds")
    def ensureutc_offset_seconds_is_zero(cls, v: int) -> int:
        """Validate that utc_offset_seconds is zero for UTC timezone."""
        if v != 0:
            raise ValueError("utc_offset_seconds must be 0 for UTC timezone")
        return v

    # Function taht create an openmeteoresponse from an instance of openmeteo_sdk api response
    @classmethod
    def from_openmeteo_sdk_response(
        cls, response: WeatherApiResponse, request: OpenMeteoRequest
    ) -> "OpenMeteoResponse":
        """Create OpenMeteoResponse from openmeteo_sdk response object."""

        # 1. First create a dataframe withg the time as index without any variable
        df = pd.DataFrame()
        df["timestamp"] = pd.date_range(
            start=pd.to_datetime(response.Hourly().Time(), unit="s", utc=True),  # ty: ignore[possibly-missing-attribute]
            end=pd.to_datetime(response.Hourly().TimeEnd(), unit="s", utc=True),  # ty: ignore[possibly-missing-attribute]
            freq=pd.Timedelta(seconds=response.Hourly().Interval()),  # ty: ignore[possibly-missing-attribute]
            inclusive="left",
        )
        units = {}
        df.set_index("timestamp", inplace=True)

        # 2. Add a column for each variable requested in the request
        for i, var in enumerate(request.hourly):
            df[var] = response.Hourly().Variables(i).ValuesAsNumpy()  # ty: ignore[possibly-missing-attribute]
            units[var] = UNIT_VALUE_TO_NAME.get(response.Hourly().Variables(i).Unit())  # ty: ignore[possibly-missing-attribute]

        # 3. Crate the instance
        return cls(
            latitude=response.Latitude(),
            longitude=response.Longitude(),
            generationtime_ms=response.GenerationTimeMilliseconds(),
            utc_offset_seconds=response.UtcOffsetSeconds(),
            elevation=response.Elevation(),
            data=df,
            units=units,
        )
