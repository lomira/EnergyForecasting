"""Weather source and storage configuration."""

from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
DB_PATH = WORKSPACE_ROOT / "db" / "weather.sqlite3"
CACHE_PATH = WORKSPACE_ROOT / "data" / ".cache_meteo"
WEATHER_PREVIOUS_DAYS = 2
WEATHER_METRICS = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "shortwave_radiation",
)
WEATHER_API_PARAMS = tuple(
    name
    for metric in WEATHER_METRICS
    for name in (
        metric,
        *(f"{metric}_previous_day{day}" for day in range(1, WEATHER_PREVIOUS_DAYS + 1)),
    )
)
CITIES = (
    {
        "name": "Alger",
        "region": "Nord",
        "lat": 36.73,
        "lon": 3.08,
        "weight": 2_364_230,
    },
    {
        "name": "Constantine",
        "region": "Milieu",
        "lat": 36.365,
        "lon": -3.74,
        "weight": 448_028,
    },
    {
        "name": "Djelfa",
        "region": "Sud-Est",
        "lat": 34.67,
        "lon": 3.26,
        "weight": 265_833,
    },
)
