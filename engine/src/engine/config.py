from pathlib import Path

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_project_root() -> Path:
    """Return the repository root for the ENTIRE project."""
    # TODO Probably not a good idea
    return Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    """Egine Application settings."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    project_root: Path = Field(default_factory=_default_project_root)

    @computed_field
    @property
    def data_root(self) -> Path:
        """Root directory for all data files."""
        return self.project_root / "data"

    @computed_field
    @property
    def raw_data_root(self) -> Path:
        """Root directory for raw input files such as Excel exports."""
        return self.data_root / "raw"

    @computed_field
    @property
    def raw_excel_root(self) -> Path:
        """Directory for raw Excel files."""
        return self.raw_data_root / "excel"

    @computed_field
    @property
    def db_root(self) -> Path:
        """Directory for database files."""
        return self.data_root / "db"

    @computed_field
    @property
    def duckdb_path(self) -> Path:
        """Location of the DuckDB database file."""
        return self.db_root / "energy_forecast.duckdb"

    def ensure_directories(self) -> None:
        """Create the directories if they do not exist."""
        for path in (
            self.data_root,
            self.raw_data_root,
            self.raw_excel_root,
            self.db_root,
        ):
            path.mkdir(parents=True, exist_ok=True)


settings = Settings()
settings.ensure_directories()
