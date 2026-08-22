import pickle
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import pandas as pd
from darts import TimeSeries

from engine.forecasting.runner import backtest_metrics
from engine.forecasting.spec import BacktestSpec
from engine.model_configs import model_hourly
from engine.storage.backtests import (
    list_backtest_results,
    read_backtest_result,
    save_backtest_result,
)


class BacktestStoreTests(TestCase):
    def setUp(self) -> None:
        self.index = pd.date_range("2024-01-01", periods=48, freq="h")
        self.actual = TimeSeries.from_times_and_values(self.index, range(1, 49))
        self.forecast = TimeSeries.from_times_and_values(self.index[23::24], [25, 46])
        self.spec = BacktestSpec(
            forecast_horizon=24,
            stride=24,
            retrain=True,
            start=self.index[0],
        )
        self.config = model_hourly["lightgbm_nex"]
        self.metrics = backtest_metrics(self.actual, self.forecast)

    def test_backtest_round_trip_and_listing(self) -> None:
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "internal.sqlite3"
            first_id = save_backtest_result(
                "lightgbm_nex",
                self.config,
                self.spec,
                self.actual,
                self.forecast,
                metrics=self.metrics,
                db_path=db_path,
            )
            second_id = save_backtest_result(
                "second_config",
                self.config,
                self.spec,
                self.actual,
                self.forecast,
                metrics=self.metrics,
                db_path=db_path,
            )

            retained = read_backtest_result(first_id, db_path=db_path)
            listed = list_backtest_results(db_path=db_path)
            filtered = list_backtest_results("lightgbm_nex", db_path=db_path)

        self.assertEqual(retained.configuration_name, "lightgbm_nex")
        self.assertIs(retained.config["model_cls"], self.config["model_cls"])
        self.assertEqual(retained.spec, self.spec)
        self.assertEqual(retained.curve["actual"].tolist(), [24.0, 48.0])
        self.assertEqual(retained.curve["forecast"].tolist(), [25.0, 46.0])
        self.assertEqual(retained.metrics["hourly_mae"], 1.5)
        self.assertEqual(listed["backtest_id"].tolist(), [second_id, first_id])
        self.assertIn("monthly_peak_wape", listed)
        self.assertEqual(filtered["backtest_id"].tolist(), [first_id])

    def test_rejects_missing_actual_timestamp_before_writing(self) -> None:
        forecast = TimeSeries.from_times_and_values(
            pd.date_range(self.index[-1] + pd.Timedelta(hours=1), periods=2, freq="h"),
            [1, 2],
        )
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "internal.sqlite3"
            with self.assertRaisesRegex(ValueError, "cover every forecast timestamp"):
                save_backtest_result(
                    "lightgbm_nex",
                    self.config,
                    self.spec,
                    self.actual,
                    forecast,
                    metrics=self.metrics,
                    db_path=db_path,
                )
            self.assertFalse(db_path.exists())

    def test_unknown_backtest_raises(self) -> None:
        with (
            TemporaryDirectory() as directory,
            self.assertRaisesRegex(ValueError, "Unknown backtest result"),
        ):
            read_backtest_result(
                "missing", db_path=Path(directory) / "internal.sqlite3"
            )

    def test_registered_configs_are_pickle_compatible(self) -> None:
        for name, config in model_hourly.items():
            with self.subTest(name=name):
                restored = pickle.loads(pickle.dumps(config))
                self.assertIs(restored["model_cls"], config["model_cls"])
                self.assertEqual(restored["train_length"], config["train_length"])
