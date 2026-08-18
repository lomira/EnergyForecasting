import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
from engine.ingestion.internal_db import (
    populate_internal_db,
    read_corrected_load,
    read_future_covariates,
)


class InternalDatabaseTests(TestCase):
    @patch("engine.ingestion.internal_db.holiday_data.read")
    @patch("engine.ingestion.internal_db.weather_data.read")
    @patch("engine.ingestion.internal_db.load_data.read")
    def test_population_replaces_both_internal_tables(
        self, read_load, read_weather, read_holidays
    ) -> None:
        load_index = pd.date_range("2024-01-01", periods=300, freq="h")
        weather_index = load_index[100:]
        read_load.return_value = pd.DataFrame(
            {"load_mw": range(300)}, index=load_index
        )
        read_weather.return_value = pd.DataFrame(
            {"Alger_temperature_2m": range(200)}, index=weather_index
        )
        read_holidays.return_value = pd.DataFrame(
            {"holidays": False}, index=weather_index
        )

        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "internal.sqlite3"
            self.assertIsNone(populate_internal_db(db_path=db_path))

            corrected = read_corrected_load(db_path=db_path)
            covariates = read_future_covariates(
                weather_index[0], weather_index[-1], db_path=db_path
            )
            self.assertEqual(len(corrected), 300)
            self.assertEqual(len(covariates), 200)
            self.assertEqual(
                len(
                    read_corrected_load(
                        load_index[1], load_index[2], db_path=db_path
                    )
                ),
                2,
            )
            self.assertFalse(covariates.isna().any().any())
            self.assertIn(
                "Alger_temperature_2m__roll_mean168_lag24", covariates
            )
            self.assertTrue(
                all(f"custom_weekday_{day}" in covariates for day in range(1, 5))
            )
            with sqlite3.connect(db_path) as connection:
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
            self.assertEqual(tables, {"corrected_load", "future_covariates"})

            read_load.return_value = read_load.return_value.iloc[1:]
            read_weather.return_value = read_weather.return_value.iloc[1:]
            read_holidays.return_value = read_holidays.return_value.iloc[1:]
            populate_internal_db(db_path=db_path)
            self.assertEqual(len(read_corrected_load(db_path=db_path)), 299)
            self.assertEqual(
                len(
                    read_future_covariates(
                        weather_index[1], weather_index[-1], db_path=db_path
                    )
                ),
                199,
            )
