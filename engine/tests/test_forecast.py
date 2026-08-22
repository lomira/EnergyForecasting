from unittest import TestCase
from unittest.mock import MagicMock, patch

import pandas as pd
from darts import TimeSeries

from engine.forecasting.runner import run_forecast


class ForecastTests(TestCase):
    @patch("engine.forecasting.runner.build_model")
    @patch("engine.forecasting.runner.build_data_transformers")
    def test_forecast_uses_training_window_and_transformers(
        self, build_transformers, build_model
    ) -> None:
        index = pd.date_range("2024-01-01", periods=6, freq="h")
        series = TimeSeries.from_times_and_values(index, range(6))
        covariate_index = pd.date_range(index[0], periods=8, freq="h")
        historical_covariates = TimeSeries.from_dataframe(
            pd.DataFrame(
                {"temperature": range(7), "unused": range(100, 107)},
                index=covariate_index[:-1],
            )
        )
        future_scenario = TimeSeries.from_dataframe(
            pd.DataFrame(
                {"temperature": [60, 70], "unused": [160, 170]},
                index=covariate_index[-2:],
            )
        )
        covariates = TimeSeries.from_dataframe(
            pd.DataFrame(
                {"temperature": [0, 1, 2, 3, 4, 5, 60, 70]},
                index=covariate_index,
            )
        )
        transformed_forecast = TimeSeries.from_times_and_values(
            pd.date_range("2024-01-01 06:00", periods=2, freq="h"), [7, 8]
        )
        forecast = TimeSeries.from_times_and_values(
            transformed_forecast.time_index, [70, 80]
        )

        target_transformer = MagicMock()
        target_transformer.fit_transform.side_effect = lambda value: value
        target_transformer.inverse_transform.return_value = forecast
        covariate_transformer = MagicMock()
        covariate_transformer.transform.side_effect = lambda value: value
        build_transformers.return_value = {
            "series": target_transformer,
            "future_covariates": covariate_transformer,
        }
        model = build_model.return_value
        model.supports_future_covariates = True
        model.predict.return_value = transformed_forecast

        result = run_forecast(
            {"train_length": 3, "feature_subset": ("temperature",)},
            series,
            2,
            future_cov=historical_covariates,
            future_scenario=future_scenario,
        )

        training_series = target_transformer.fit_transform.call_args.args[0]
        self.assertEqual(training_series.time_index[0], index[-3])
        self.assertEqual(len(training_series), 3)
        fitted_covariates = covariate_transformer.fit.call_args.args[0]
        self.assertTrue(fitted_covariates.time_index.equals(training_series.time_index))
        self.assertEqual(list(fitted_covariates.components), ["temperature"])
        model.fit.assert_called_once()
        model.predict.assert_called_once_with(
            n=2,
            future_covariates=covariates,
        )
        self.assertIs(result, forecast)

    @patch("engine.forecasting.runner.build_model")
    @patch("engine.forecasting.runner.build_data_transformers", return_value={})
    def test_forecast_ignores_covariates_for_empty_feature_subset(
        self, _build_transformers, build_model
    ) -> None:
        index = pd.date_range("2024-01-01", periods=6, freq="h")
        series = TimeSeries.from_times_and_values(index, range(6))
        covariates = TimeSeries.from_times_and_values(index, range(6))
        forecast = TimeSeries.from_times_and_values(
            pd.date_range("2024-01-01 06:00", periods=2, freq="h"), [7, 8]
        )
        model = build_model.return_value
        model.supports_future_covariates = False
        model.predict.return_value = forecast

        run_forecast(
            {"train_length": 3},
            series,
            2,
            future_cov=covariates,
            future_scenario=covariates[-2:],
        )

        model.fit.assert_called_once_with(series[-3:], future_covariates=None)
        model.predict.assert_called_once_with(n=2, future_covariates=None)

    @patch("engine.forecasting.runner.build_model")
    def test_forecast_rejects_features_for_unsupported_model(self, build_model) -> None:
        index = pd.date_range("2024-01-01", periods=6, freq="h")
        series = TimeSeries.from_times_and_values(index, range(6))
        build_model.return_value.supports_future_covariates = False

        with self.assertRaisesRegex(ValueError, "feature_subset is not empty"):
            run_forecast(
                {"train_length": 3, "feature_subset": ("temperature",)},
                series,
                2,
            )

    @patch("engine.forecasting.runner.build_model")
    def test_forecast_rejects_missing_feature(self, build_model) -> None:
        index = pd.date_range("2024-01-01", periods=6, freq="h")
        series = TimeSeries.from_times_and_values(index, range(6))
        build_model.return_value.supports_future_covariates = True

        with self.assertRaisesRegex(ValueError, "missing configured components"):
            run_forecast(
                {"train_length": 3, "feature_subset": ("temperature",)},
                series,
                2,
                future_cov=series,
            )
