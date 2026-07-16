"""Settings for the django app of engine"""

from pathlib import Path

# WORKSPACE ROOT
# django_settings.py lives at <root>/engine/src/engine/django_settings.py
# so parents[3] is the workspace root (EnergyForecasting/).
BASE_DIR = Path(__file__).resolve().parents[3]

# PATHS
ENGINE_DATA_ROOT = BASE_DIR / "data"
ENGINE_RAW_EXCEL_ROOT = BASE_DIR / "data" / "raw" / "excel"
ENGINE_DB_ROOT = BASE_DIR / "db"
ENGINE_SQLITE_FILENAME = "energy_forecast.sqlite3"

# WEATHER
ENGINE_CACHE_METEO = str(BASE_DIR / "data" / ".cache_meteo")
ENGINE_WEATHER_METRICS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "shortwave_radiation",
]
ENGINE_WEATHER_PREV_DAYS = 2

ENGINE_VILLES = {
    "0": {
        "name": "Alger",
        "region": "Nord",
        "lat": 36.73,
        "lon": 3.08,
        "weight": 2_364_230,
    },
    "1": {
        "name": "Constantine",
        "region": "Milieu",
        "lat": 36.365,
        "lon": -3.74,
        "weight": 448_028,
    },
    "2": {
        "name": "Djelfa",
        "region": "Sud-Est",
        "lat": 34.67,
        "lon": 3.26,
        "weight": 265_833,
    },
}

# DJANGO
SECRET_KEY = "engine-standalone-not-for-production"
DEBUG = False
USE_TZ = False  # time-series data are alwayse stored tz-naive

INSTALLED_APPS = [
    "engine",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": str(ENGINE_DB_ROOT / ENGINE_SQLITE_FILENAME),
    }
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Disable migrations: the database is rebuilt from scratch on every run, so we
# create the tables directly from the model definitions instead of tracking
# migrations. `migrate` (and the test runner) will build the schema from models.
MIGRATION_MODULES = {"engine": None}
