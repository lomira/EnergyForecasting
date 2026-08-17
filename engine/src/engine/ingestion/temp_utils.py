from pathlib import Path

import holiday_data
import load_data
import weather_data

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]


def populate_dbs() -> None:
    if not load_data.DB_PATH.exists():
        load_data.import_excel(
            file_path=WORKSPACE_ROOT / "data" / "raw" / "excel" / "BDD_E.xlsx",
            sheet_name="Feuil1",
        )

    start_date, end_date = load_data.get_date_range()

    if not holiday_data.DB_PATH.exists():
        holiday_data.sync(start_date, end_date)

    if not weather_data.DB_PATH.exists():
        weather_data.sync(start_date, end_date)
