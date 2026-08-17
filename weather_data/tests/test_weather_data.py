from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import pandas as pd
from weather_data.store import _upsert as upsert

from weather_data import read


class WeatherDataTests(TestCase):
    def test_storage_is_idempotent_and_read_pivots_cities(self) -> None:
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "nested" / "weather.sqlite3"
            timestamp = pd.Timestamp("2024-01-01 00:00")
            rows = {
                (timestamp, "Alger"): {
                    "datetime": timestamp,
                    "city": "Alger",
                    "temperature_2m": 10.0,
                    "relative_humidity_2m": 60.0,
                },
                (timestamp, "Constantine"): {
                    "datetime": timestamp,
                    "city": "Constantine",
                    "temperature_2m": 11.0,
                    "relative_humidity_2m": 55.0,
                },
            }
            upsert(rows, db_path=db_path)
            rows[(timestamp, "Alger")]["temperature_2m"] = 12.0
            upsert(rows, db_path=db_path)

            data = read(
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-01"),
                db_path=db_path,
            )
            self.assertTrue(db_path.exists())
            self.assertEqual(len(data), 1)
            self.assertEqual(data.iloc[0]["Alger_temperature_2m"], 12.0)
            self.assertEqual(
                data.iloc[0]["Constantine_relative_humidity_2m"], 55.0
            )
