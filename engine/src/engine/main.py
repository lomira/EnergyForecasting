import os
from datetime import datetime

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "engine.django_settings")
django.setup()

from django.conf import settings  # noqa: E402
from django.core.management import call_command  # noqa: E402

from engine.ingestion.get_all_covariates import get_all_covariates  # noqa: E402
from engine.ingestion.get_holidays import get_holidays  # noqa: E402
from engine.ingestion.load_ingestion import (  # noqa: E402
    add_load_excel_to_db,
    get_load_start_end_dates,
)
from engine.ingestion.weather_ingestion import get_weather_data  # noqa: E402
from engine.logging_config import logger, setup_logging  # noqa: E402

setup_logging()

if __name__ == "__main__":
    # Remove the database to ensure a clean rebuild each time
    # The weather cache is kept separate
    db_path = settings.ENGINE_DB_ROOT / settings.ENGINE_SQLITE_FILENAME
    db_path.unlink(missing_ok=True)

    # Ensure the DB directory exists
    # Then create the schema directly from the model definitions
    # Migrations are disabled in settings
    settings.ENGINE_DB_ROOT.mkdir(parents=True, exist_ok=True)
    call_command("migrate", run_syncdb=True, verbosity=1)
    logger.info(f"Created SQLite database at {db_path}")

    add_load_excel_to_db(
        file_path=settings.ENGINE_RAW_EXCEL_ROOT / "BDD_E.xlsx",
        sheet_name="Feuil1",
        db_path=db_path,
    )

    start_date, end_date = get_load_start_end_dates(db_path)
    start_date = datetime(2016, 1, 1)  # Overrides the start date
    get_holidays(start_date, end_date)
    get_weather_data(start_date, end_date)

    cov = get_all_covariates(start_date, end_date)
    logger.info(f"Built covariate frame with {len(cov):,.0f} rows")
    print(cov)
