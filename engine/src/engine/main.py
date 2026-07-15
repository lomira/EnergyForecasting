from datetime import datetime

from engine.database.initialise import create_database
from engine.ingestion.get_all_covariates import get_all_covariates
from engine.ingestion.load_ingestion import (
    add_load_excel_to_db,
    get_load_start_end_dates,
)

if __name__ == "__main__":
    created_path = create_database()
    print(f"Created SQLite database at {created_path}")

    add_load_excel_to_db(
        file_path="data/raw/excel/BDD_E.xlsx",
        sheet_name="Feuil1",
        db_path=created_path,
    )

    start_date, end_date = get_load_start_end_dates(created_path)
    start_date = datetime(2016, 1, 1)

    print(get_all_covariates(start_date, end_date))
