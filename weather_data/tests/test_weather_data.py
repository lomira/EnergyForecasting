from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pandas as pd
from weather_data.store import _upsert as upsert

from weather_data import WEATHER_METRICS, read, sync


def _response(timestamp: pd.Timestamp, values: list[float]):
    hourly = MagicMock()
    hourly.Time.return_value = timestamp.timestamp()
    hourly.TimeEnd.return_value = (timestamp + pd.Timedelta(hours=1)).timestamp()
    hourly.Interval.return_value = 3600
    hourly.Variables.side_effect = lambda index: MagicMock(
        ValuesAsNumpy=MagicMock(return_value=[values[index]])
    )
    response = MagicMock()
    response.Hourly.return_value = hourly
    return [response]


class WeatherDataTests(TestCase):
    def test_read_uses_archive_only_before_previous_runs_begin(self) -> None:
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "weather.sqlite3"
            before = pd.Timestamp("2023-12-31 23:00")
            missing = pd.Timestamp("2024-01-01 00:00")
            available = pd.Timestamp("2024-01-01 01:00")
            rows = {
                (before, "Alger"): {
                    "datetime": before,
                    "city": "Alger",
                    "temperature_2m": 10.0,
                    "temperature_2m_previous_day1": None,
                },
                (missing, "Alger"): {
                    "datetime": missing,
                    "city": "Alger",
                    "temperature_2m": 20.0,
                    "temperature_2m_previous_day1": None,
                },
                (available, "Alger"): {
                    "datetime": available,
                    "city": "Alger",
                    "temperature_2m": 30.0,
                    "temperature_2m_previous_day1": 18.0,
                },
            }
            upsert(rows, db_path=db_path)
            data = read(before, available, db_path=db_path)

        self.assertEqual(data.loc[before, "Alger_temperature_2m_previous_day1"], 10.0)
        self.assertTrue(
            pd.isna(data.loc[missing, "Alger_temperature_2m_previous_day1"])
        )
        self.assertEqual(
            data.loc[available, "Alger_temperature_2m_previous_day1"], 18.0
        )

    def test_sync_starts_previous_runs_at_2024(self) -> None:
        timestamp = pd.Timestamp("2008-01-01 00:00")
        openmeteo = MagicMock()
        openmeteo.weather_api.side_effect = (
            _response(timestamp, [10.0, 60.0, 0.0, 5.0, 100.0]),
            _response(pd.Timestamp("2024-01-01"), [1.0] * 10),
        )
        city = {"name": "Alger", "lat": 36.73, "lon": 3.08}

        with (
            TemporaryDirectory() as directory,
            patch("weather_data.ingestion.CITIES", (city,)),
            patch(
                "weather_data.ingestion.openmeteo_requests.Client",
                return_value=openmeteo,
            ),
            patch("weather_data.ingestion._upsert"),
        ):
            sync(
                timestamp,
                pd.Timestamp("2025-01-01"),
                cache_path=Path(directory) / "weather-cache",
            )

        archive_call, previous_call = openmeteo.weather_api.call_args_list
        self.assertEqual(archive_call.kwargs["params"]["start_date"], "2008-01-01")
        self.assertEqual(previous_call.kwargs["params"]["start_date"], "2024-01-01")
        self.assertEqual(archive_call.kwargs["params"]["models"], "era5")
        self.assertEqual(
            previous_call.kwargs["params"]["models"], "ecmwf_ifs025"
        )

    def test_sync_merges_archive_actuals_with_previous_runs(self) -> None:
        timestamp = pd.Timestamp("2024-01-01 00:00")
        openmeteo = MagicMock()
        openmeteo.weather_api.side_effect = (
            _response(timestamp, [10.0, 60.0, 0.0, 5.0, 100.0]),
            _response(timestamp, [float("nan"), 8.0, *([0.0] * 8)]),
        )
        city = {"name": "Alger", "lat": 36.73, "lon": 3.08}

        with (
            TemporaryDirectory() as directory,
            patch("weather_data.ingestion.CITIES", (city,)),
            patch(
                "weather_data.ingestion.openmeteo_requests.Client",
                return_value=openmeteo,
            ),
            patch("weather_data.ingestion._upsert") as mocked_upsert,
        ):
            self.assertEqual(
                sync(
                    timestamp,
                    timestamp,
                    cache_path=Path(directory) / "weather-cache",
                ),
                1,
            )

        archive_call, previous_call = openmeteo.weather_api.call_args_list
        self.assertEqual(
            archive_call.args[0],
            "https://archive-api.open-meteo.com/v1/archive",
        )
        self.assertEqual(archive_call.kwargs["params"]["hourly"], WEATHER_METRICS)
        self.assertEqual(
            previous_call.args[0],
            "https://previous-runs-api.open-meteo.com/v1/forecast",
        )
        self.assertTrue(
            all(
                "_previous_day" in param
                for param in previous_call.kwargs["params"]["hourly"]
            )
        )
        row = mocked_upsert.call_args.args[0][(timestamp, "Alger")]
        self.assertEqual(row["temperature_2m"], 10.0)
        self.assertTrue(pd.isna(row["temperature_2m_previous_day1"]))
        self.assertEqual(row["temperature_2m_previous_day2"], 8.0)

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
            self.assertEqual(data.iloc[0]["Constantine_relative_humidity_2m"], 55.0)
            self.assertIn("Alger_temperature_2m_previous_day1", data)
            self.assertTrue(pd.isna(data.iloc[0]["Alger_temperature_2m_previous_day1"]))
