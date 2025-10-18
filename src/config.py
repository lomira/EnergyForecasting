from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class AppSettings(BaseSettings):
    data_dir: Path = Field(default=Path("data"), alias="DATA_DIR")
    raw_dir: Path = Field(default=Path("raw"), alias="RAW_DIR")
    weather_dir: Path = Field(default=Path("weather"), alias="WEATHER_DIR")

    supported_file_extensions: list[str] = Field(
        default=[".csv"], alias="SUPPORTED_FILE_EXTENSIONS"
    )
    allowed_timezones: list[str] = Field(default=["UTC", "Europe/Paris"], alias="ALLOWED_TIMEZONES")

    granularity_freq_map: dict[str, str] = Field(
        default={
            "hourly": "H",
            "daily": "D",
            "monthly": "M",
        },
        alias="GRANULARITY_FREQ_MAP",
    )

    weather_city_names: list[str] = Field(
        default=["Abidjan, Côte d'Ivoire", "Yamoussoukro,Côte d'Ivoire"],
        alias="WEATHER_CITY_NAMES",
    )

    weather_variables: dict[str, bool] = Field(
        default_factory=lambda: {
            "temperature_2m": True,
            "relative_humidity_2m": True,
            "precipitation": True,
            "wind_speed_10m": True,
            "wind_direction_10m": True,
            "cloud_cover": True,
            "surface_pressure": True,
        },
        alias="WEATHER_VARIABLES",
    )

    api_base: str = Field(default="http://localhost:8000/api/v1", alias="API_BASE")

    @property
    def granularity_options(self) -> list[str]:
        """Get list of supported granularity options."""
        return list(self.granularity_freq_map.keys())

    @property
    def weather_variables_options(self) -> list[str]:
        """Get list of weather variables to fetch."""
        return [var for var, include in self.weather_variables.items() if include]

    def ensure_directories(self) -> None:
        """Create required directory structure if it does not exist."""
        list_directories = [
            self.data_dir,
            self.data_dir / self.raw_dir,
            self.data_dir / self.raw_dir / self.weather_dir,
        ]
        for directory in list_directories:
            directory.mkdir(parents=True, exist_ok=True)

    def __init__(self, **date) -> None:
        super().__init__(**date)
        self.ensure_directories()


def get_settings() -> AppSettings:
    """Get application settings."""
    return AppSettings()


__all__ = ["AppSettings", "get_settings"]
