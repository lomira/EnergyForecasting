from unittest import TestCase
from unittest.mock import patch

import numpy as np
import pandas as pd
from darts import TimeSeries

from engine.darts_pipeline.runner import (
    backtest_metrics,
    run_backtest,
    run_forecast,
)
from engine.darts_pipeline.spec import BacktestSpec
from engine.peak_reconciled_lightgbm import (
    PeakReconciledLightGBMModel,
    aggregate_blocks,
    reconcile_daily_peaks,
)


class PeakReconciledLightGBMTests(TestCase):
    def test_prediction_aligned_aggregation_and_reconciliation(self) -> None:
        index = pd.date_range("2024-01-01 07:00", periods=48, freq="h")
        hourly = TimeSeries.from_times_and_values(index, np.arange(48) + 1)

        peaks = aggregate_blocks(hourly, index[0], 2, "max")
        means = aggregate_blocks(hourly, index[0], 2, "mean")

        np.testing.assert_array_equal(peaks.univariate_values(), [24, 48])
        np.testing.assert_array_equal(means.univariate_values(), [12.5, 36.5])
        self.assertTrue(
            peaks.time_index.equals(pd.date_range(index[0], periods=2, freq="24h"))
        )

        desired = TimeSeries.from_times_and_values(peaks.time_index, [48, 24])
        reconciled = reconcile_daily_peaks(hourly, desired)
        self.assertEqual(float(reconciled[:24].values().max()), 48)
        self.assertEqual(float(reconciled[24:].values().max()), 24)
        np.testing.assert_allclose(
            reconciled[:24].univariate_values() / hourly[:24].univariate_values(),
            2,
        )
        self.assertTrue(reconciled.time_index.equals(hourly.time_index))

    def test_calendar_peak_metrics(self) -> None:
        index = pd.date_range("2023-12-31", periods=48, freq="h")
        actual_values = np.r_[
            np.arange(1, 11), np.ones(14), np.arange(1, 21), np.ones(4)
        ]
        series = TimeSeries.from_times_and_values(index, actual_values)
        forecast = TimeSeries.from_times_and_values(
            index, np.r_[12, np.ones(23), 19, np.ones(23)]
        )

        metrics = backtest_metrics(series, forecast)
        self.assertAlmostEqual(metrics["hourly_mae"], 5.5)
        self.assertAlmostEqual(metrics["hourly_bias"], -103 / 24)
        self.assertAlmostEqual(metrics["daily_peak_mae"], 1.5)
        self.assertAlmostEqual(metrics["daily_peak_wape"], 10.0)
        self.assertAlmostEqual(metrics["daily_peak_bias"], 0.5)
        self.assertAlmostEqual(metrics["monthly_peak_mae"], 1.5)
        self.assertAlmostEqual(metrics["monthly_peak_wape"], 10.0)
        self.assertAlmostEqual(metrics["monthly_peak_bias"], 0.5)

        spec = BacktestSpec(
            forecast_horizon=24,
            stride=24,
            retrain=True,
            start=index[0],
        )
        with (
            patch("engine.darts_pipeline.runner.build_model") as build_model,
            patch(
                "engine.darts_pipeline.runner.build_data_transformers",
                return_value={},
            ),
            patch("engine.darts_pipeline.runner.logger.info") as log_info,
        ):
            model = build_model.return_value
            model.supports_future_covariates = False
            model.historical_forecasts.return_value = forecast
            result = run_backtest({"train_length": 24}, spec, series)

        self.assertIs(result, forecast)
        self.assertTrue(
            model.historical_forecasts.call_args.kwargs["last_points_only"]
        )
        self.assertEqual(
            [call.args[1] for call in log_info.call_args_list],
            ["MagicMock backtest"],
        )

    def test_model_works_through_forecast_and_backtest(self) -> None:
        train_length = 24 * 10
        target_index = pd.date_range("2024-01-01", periods=train_length + 48, freq="h")
        covariate_index = target_index
        target = TimeSeries.from_times_and_values(
            target_index, 100 + np.sin(np.arange(len(target_index)) / 12)
        )
        covariates = TimeSeries.from_times_and_values(
            covariate_index,
            20 + np.cos(np.arange(len(covariate_index)) / 12),
            columns=["temperature"],
        )
        config = {
            "model_cls": PeakReconciledLightGBMModel,
            "train_length": train_length,
            "feature_subset": ("temperature",),
            "hyperparams": {
                "hourly_train_length": 24 * 3,
                "daily_train_length": 10,
                "hourly_hyperparams": {
                    "lags": [-1, -24],
                    "lags_future_covariates": [0],
                    "output_chunk_length": 24,
                    "n_estimators": 5,
                    "verbose": -1,
                },
                "daily_hyperparams": {
                    "lags": [-1, -2, -7],
                    "lags_future_covariates": [0],
                    "output_chunk_length": 1,
                    "n_estimators": 5,
                    "verbose": -1,
                },
            },
        }

        with patch(
            "engine.peak_reconciled_lightgbm.reconcile_daily_peaks",
            wraps=reconcile_daily_peaks,
        ) as reconcile:
            forecast = run_forecast(
                config, target[:train_length], 24, future_cov=covariates
            )

        daily_peak = reconcile.call_args.args[1]
        self.assertEqual(len(forecast), 24)
        self.assertEqual(forecast.freq, target.freq)
        self.assertAlmostEqual(
            float(forecast.values().max()),
            float(daily_peak.univariate_values()[0]),
        )

        backtest = run_backtest(
            config,
            BacktestSpec(
                forecast_horizon=24,
                stride=24,
                retrain=True,
                start=target_index[train_length + 24],
            ),
            target,
            future_cov=covariates,
        )
        self.assertEqual(len(backtest), 1)
