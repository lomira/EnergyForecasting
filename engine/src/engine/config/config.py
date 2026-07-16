from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, BeforeValidator, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SUBPACKAGE_ROOT = Path(__file__).resolve().parent


def _resolve_path(v: str | Path) -> Path:
    """Ensure relative paths are resolved against the absolute project root."""
    path = Path(v).expanduser()
    return path if path.is_absolute() else (PROJECT_ROOT / path).resolve()


# Custom type that automatically forces absolute path resolution
AbsPath = Annotated[Path, BeforeValidator(_resolve_path)]


class Ville(BaseModel):
    name: str
    region: str
    lat: float
    lon: float
    weight: int


@dataclass
class Tables:
    load: str = "load_time_series"
    weather: str = "weather"
    weather_tidy: str = "weather_tidy"
    holidays: str = "holidays"


class Settings(BaseSettings):
    """Engine application settings."""

    tables: Tables = Field(default_factory=Tables)

    model_config = SettingsConfigDict(
        env_file=(
            SUBPACKAGE_ROOT / ".env",
            SUBPACKAGE_ROOT / "path.env",
            SUBPACKAGE_ROOT / "weather.env",
        ),
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
        case_sensitive=False,
    )

    project_root: AbsPath = Field(default_factory=lambda: PROJECT_ROOT)
    data_root: AbsPath
    raw_data_root: AbsPath
    raw_excel_root: AbsPath
    db_root: AbsPath

    sqlite_filename: str = Field(default="energy_forecast.sqlite3")
    ville: dict[str, Ville] = Field(default_factory=dict)
    cache_meteo: str | None = None
    weather_metrics: list[str] | None = None
    weather_metrics_previous_days: int | None = None

    @property
    def database_path(self) -> Path:
        """Location of the application database file."""
        return self.db_root / self.sqlite_filename

    def ensure_directories(self) -> None:
        """Create the directories if they do not exist."""
        for path in (
            self.data_root,
            self.raw_data_root,
            self.raw_excel_root,
            self.db_root,
        ):
            path.mkdir(parents=True, exist_ok=True)


# Initialize and run directory creation
settings = Settings()
settings.ensure_directories()
