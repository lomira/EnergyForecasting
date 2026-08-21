import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

import weather_data
from engine.featurize.weather import weather_features
from engine.ingestion.internal_db import (
    correct_loads_at,
    populate_internal_db,
    read_corrected_load,
    read_future_covariates,
)


class InternalDatabaseTests(TestCase):
    @patch("engine.ingestion.internal_db._future_covariates")
    @patch("engine.ingestion.internal_db.load_data.read")
    def test_correct_loads_at_updates_selected_rows(
        self, read_load, future_covariates
    ) -> None:
        index = pd.date_range("2024-01-01", periods=3, freq="h")
        read_load.return_value = pd.DataFrame({"load_mw": [100, 110, 120]}, index=index)
        future_covariates.return_value = pd.DataFrame({"temperature": [1, 2, 3]}, index=index)

        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "internal.sqlite3"
            populate_internal_db(db_path=db_path)
            correct_loads_at(
                pd.DataFrame({"datetime": [index[1]], "load_mw": [115]}),
                db_path=db_path,
            )
            corrected = read_corrected_load(db_path=db_path)

        self.assertEqual(corrected["load_mw"].tolist(), [100, 115, 120])

    @patch("engine.ingestion.internal_db._future_covariates")
    @patch("engine.ingestion.internal_db.load_data.read")
    def test_population_validates_covariates_without_rejecting_negative_values(
        self, read_load, future_covariates
    ) -> None:
        index = pd.date_range("2024-01-01", periods=3, freq="h")
        read_load.return_value = pd.DataFrame({"load_mw": [100, 110, 120]}, index=index)
        valid = pd.DataFrame({"temperature": [-5.0, 0.0, 5.0]}, index=index)
        invalid = {
            "duplicate": (
                valid.set_axis([index[0], index[0], index[2]]),
                "duplicates",
            ),
            "discontinuous": (valid.drop(index[1]), "continuous"),
            "missing": (
                valid.assign(temperature=[-5.0, None, 5.0]),
                "missing values",
            ),
        }
        for name, (data, message) in invalid.items():
            future_covariates.return_value = data
            with self.subTest(name=name), TemporaryDirectory() as directory:
                db_path = Path(directory) / "internal.sqlite3"
                with self.assertRaisesRegex(ValueError, message):
                    populate_internal_db(db_path=db_path)
                self.assertFalse(db_path.exists())

        future_covariates.return_value = valid
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "internal.sqlite3"
            populate_internal_db(db_path=db_path)
            stored = read_future_covariates(index[0], index[-1], db_path=db_path)
        self.assertEqual(stored.iloc[0]["temperature"], -5.0)

    @patch("engine.featurize.weather.weather_data.read")
    @patch("engine.ingestion.internal_db.holiday_features")
    @patch("engine.ingestion.internal_db.load_data.read")
    def test_population_stores_national_weather_averages(
        self, read_load, read_holidays, read_weather
    ) -> None:
        index = pd.date_range("2024-01-01", periods=3, freq="h")
        weather_index = pd.date_range(
            index[0] - pd.Timedelta(hours=191), index[-1], freq="h"
        )
        read_load.return_value = pd.DataFrame({"load_mw": [100, 110, 120]}, index=index)
        read_holidays.return_value = pd.DataFrame({"holidays": False}, index=index)
        city_values = {"Alger": 10.0, "Constantine": 20.0, "Djelfa": 30.0}
        read_weather.return_value = pd.DataFrame(
            {
                f"{city}_{metric}": city_values[city]
                for metric in weather_data.WEATHER_API_PARAMS
                for city in city_values
            },
            index=weather_index,
        )
        read_weather.return_value.loc[index[0], "Djelfa_precipitation"] = None
        self.assertTrue(
            pd.isna(
                weather_features(index[0], index[-1]).iloc[0][
                    "NationalAverage_precipitation"
                ]
            )
        )
        read_weather.return_value.loc[index[0], "Djelfa_precipitation"] = 30.0
        weights = {
            str(city["name"]): float(city["weight"]) for city in weather_data.CITIES
        }
        expected = sum(city_values[city] * weights[city] for city in city_values) / sum(
            weights.values()
        )

        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "internal.sqlite3"
            populate_internal_db(db_path=db_path)
            stored = read_future_covariates(index[0], index[-1], db_path=db_path)

        self.assertAlmostEqual(
            stored.iloc[0]["NationalAverage_temperature_2m"], expected
        )
        self.assertEqual(
            stored.iloc[0]["Alger_temperature_2m__roll_mean168_lag24"], 10.0
        )
        self.assertTrue(
            all(
                f"NationalAverage_{metric}" in stored
                for metric in weather_data.WEATHER_API_PARAMS
            )
        )
        self.assertEqual(read_weather.call_count, 2)
        read_weather.assert_called_with(weather_index[0], index[-1])

    @patch("engine.ingestion.internal_db.load_data.read")
    def test_population_rejects_incomplete_external_load(self, read_load) -> None:
        index = pd.date_range("2024-01-01", periods=3, freq="h")
        valid = pd.DataFrame({"load_mw": [100.0, 110.0, 120.0]}, index=index)
        cases = {
            "negative": (valid.assign(load_mw=[100.0, -1.0, 120.0]), "non-negative"),
            "duplicate": (
                valid.set_axis([index[0], index[0], index[2]]),
                "duplicates",
            ),
            "discontinuous": (valid.drop(index[1]), "continuous"),
            "missing": (valid.assign(load_mw=[100.0, None, 120.0]), "missing"),
        }
        for name, (data, message) in cases.items():
            read_load.return_value = data
            with (
                self.subTest(name=name),
                TemporaryDirectory() as directory,
                self.assertRaisesRegex(ValueError, message),
            ):
                populate_internal_db(db_path=Path(directory) / "internal.sqlite3")

    @patch("engine.ingestion.internal_db._future_covariates")
    @patch("engine.ingestion.internal_db.load_data.read")
    def test_population_replaces_both_internal_tables(
        self, read_load, future_covariates
    ) -> None:
        load_index = pd.date_range("2024-01-01", periods=300, freq="h")
        weather_index = load_index[100:]
        read_load.return_value = pd.DataFrame({"load_mw": range(300)}, index=load_index)
        future_covariates.return_value = pd.DataFrame(
            {
                "Alger_temperature_2m__roll_mean168_lag24": range(200),
                **{f"custom_weekday_{day}": False for day in range(1, 5)},
            },
            index=weather_index,
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
                len(read_corrected_load(load_index[1], load_index[2], db_path=db_path)),
                2,
            )
            self.assertFalse(covariates.isna().any().any())
            self.assertIn("Alger_temperature_2m__roll_mean168_lag24", covariates)
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
            future_covariates.return_value = future_covariates.return_value.iloc[1:]
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
