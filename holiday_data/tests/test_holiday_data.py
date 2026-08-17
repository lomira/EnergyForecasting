import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import pandas as pd

from holiday_data import read, sync


class HolidayDataTests(TestCase):
    def test_sync_creates_database_and_is_idempotent(self) -> None:
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "nested" / "holiday.sqlite3"
            start = pd.Timestamp("2024-01-01")
            end = pd.Timestamp("2024-01-02")

            self.assertEqual(sync(start, end, db_path=db_path), 2)
            self.assertEqual(sync(start, end, db_path=db_path), 2)

            data = read(start, end, db_path=db_path)
            self.assertTrue(db_path.exists())
            with sqlite3.connect(db_path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM holiday_observation"
                    ).fetchone()[0],
                    2,
                )
            self.assertEqual(len(data), 25)
            self.assertTrue(data.iloc[0]["holidays"])
            self.assertFalse(data.iloc[-1]["holidays"])
