from unittest import TestCase
from unittest.mock import patch

import pandas as pd
from darts import TimeSeries
from engine.darts_pipeline.runner import run_backtest
from engine.darts_pipeline.spec import BacktestSpec
from engine.model_configs import model_hourly


class TrainingLengthTests(TestCase):
    def test_models_define_their_training_length(self) -> None:
        self.assertEqual(
            {config["train_length"] for config in model_hourly.values()},
            {24 * 21},
        )

    def test_models_register_by_function_name(self) -> None:
        self.assertEqual(
            set(model_hourly),
            {"lightgbm_V1", "lightgbm_nex", "tft_V1", "nbeats_V1"},
        )

    def test_backtest_uses_the_model_training_length(self) -> None:
        series = TimeSeries.from_times_and_values(
            pd.date_range("2024-01-01", periods=3, freq="h"), [1, 2, 3]
        )
        forecast = series[-1:]
        spec = BacktestSpec(
            forecast_horizon=24,
            stride=168,
            retrain=True,
            start=pd.Timestamp("2024-01-01"),
        )

        with (
            patch("engine.darts_pipeline.runner.build_model") as build_model,
            patch(
                "engine.darts_pipeline.runner.build_data_transformers", return_value={}
            ),
            patch("engine.darts_pipeline.runner.logger.info") as log_info,
        ):
            model = build_model.return_value
            model.supports_future_covariates = False
            model.historical_forecasts.return_value = forecast
            result = run_backtest({"train_length": 123}, spec, series)

        self.assertIs(result, forecast)
        self.assertEqual(
            model.historical_forecasts.call_args.kwargs["train_length"], 123
        )
        self.assertIn("WAPE: 0.0000 (0.00%), MAPE: 0.0000 (0.00%)", log_info.call_args.args[0])
