from datetime import datetime

from engine.config.config import settings
from engine.database.initialise import create_database
from engine.ingestion.get_all_covariates import get_all_covariates
from engine.ingestion.get_holidays import get_holidays
from engine.ingestion.load_ingestion import (
    add_load_excel_to_db,
    get_load_start_end_dates,
)
from engine.ingestion.weather_ingestion import get_weather_data

if __name__ == "__main__":
    # remove the database to ensure
    file_path = settings.database_path.resolve()
    file_path.unlink(missing_ok=True)

    created_path = create_database()
    print(f"Created SQLite database at {created_path}")

    add_load_excel_to_db(
        file_path="data/raw/excel/BDD_E.xlsx",
        sheet_name="Feuil1",
        db_path=created_path,
    )

    start_date, end_date = get_load_start_end_dates(created_path)
    start_date = datetime(2016, 1, 1)  # Overrides the start date
    get_holidays(start_date, end_date)
    get_weather_data(start_date, end_date)

    cov = get_all_covariates(start_date, end_date)
    print(cov)
