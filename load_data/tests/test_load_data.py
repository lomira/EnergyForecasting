from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import pandas as pd

from load_data import format_load_data, get_date_range, import_excel, read


class LoadDataTests(TestCase):
    def test_real_excel_file_can_be_imported(self) -> None:
        excel_path = Path(__file__).resolve().parents[2] / "data/raw/excel/BDD_E.xlsx"
        with TemporaryDirectory() as directory:
            self.assertGreater(
                import_excel(excel_path, db_path=Path(directory) / "load.sqlite3"), 0
            )

    def test_excel_import_creates_database_and_upserts(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            excel_path = root / "load.xlsx"
            db_path = root / "nested" / "load.sqlite3"
            source = pd.DataFrame(
                {
                    "Date": [pd.Timestamp("2024-01-01")],
                    "1h": [100.0],
                    "2h": [110.0],
                }
            )
            source.to_excel(excel_path, index=False, sheet_name="Feuil1")

            self.assertEqual(import_excel(excel_path, db_path=db_path), 2)
            source.loc[0, "1h"] = 120.0
            source.to_excel(excel_path, index=False, sheet_name="Feuil1")
            self.assertEqual(import_excel(excel_path, db_path=db_path), 2)

            data = read(db_path=db_path)
            self.assertTrue(db_path.exists())
            self.assertEqual(len(data), 2)
            self.assertEqual(data.iloc[0]["load_mw"], 120.0)
            self.assertEqual(
                get_date_range(db_path=db_path),
                (
                    pd.Timestamp("2024-01-01 00:00"),
                    pd.Timestamp("2024-01-01 01:00"),
                ),
            )
            self.assertTrue(
                all(
                    isinstance(value, pd.Timestamp)
                    for value in get_date_range(db_path=db_path)
                )
            )

    def test_negative_load_is_rejected(self) -> None:
        source = pd.DataFrame({"Date": [pd.Timestamp("2024-01-01")], "1h": [-1.0]})
        with self.assertRaisesRegex(ValueError, "non-negative"):
            format_load_data(source)
