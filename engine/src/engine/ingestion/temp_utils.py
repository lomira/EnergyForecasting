from pathlib import Path

import holiday_data
import load_data
import weather_data
from engine.featurize.weather import WEATHER_LOOKBACK

WORKSPACE_ROOT = Path(__file__).resolve().parents[4]


def populate_externals_dbs() -> None:
    if not load_data.DB_PATH.exists():
        load_data.import_excel(
            file_path=WORKSPACE_ROOT / "data" / "raw" / "excel" / "BDD_E.xlsx",
            sheet_name="Feuil1",
        )

    start_date, end_date = load_data.get_date_range()

    if not holiday_data.DB_PATH.exists():
        holiday_data.sync(start_date, end_date)

    if not weather_data.DB_PATH.exists():
        weather_data.sync(start_date - WEATHER_LOOKBACK, end_date)
