from unittest import TestCase
from unittest.mock import MagicMock, patch

import pandas as pd
from darts import TimeSeries
from engine.darts_pipeline.runner import run_forecast


class ForecastTests(TestCase):
    @patch("engine.darts_pipeline.runner.build_model")
    @patch("engine.darts_pipeline.runner.build_data_transformers")
    def test_forecast_uses_training_window_and_transformers(
        self, build_transformers, build_model
    ) -> None:
        index = pd.date_range("2024-01-01", periods=6, freq="h")
        series = TimeSeries.from_times_and_values(index, range(6))
        covariate_index = pd.date_range(index[0], periods=8, freq="h")
        historical_covariates = TimeSeries.from_times_and_values(
            covariate_index, range(8)
        )
        future_scenario = TimeSeries.from_times_and_values(
            covariate_index[-2:], [60, 70]
        )
        covariates = TimeSeries.from_times_and_values(
            covariate_index, [0, 1, 2, 3, 4, 5, 60, 70]
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
        model.predict.return_value = transformed_forecast

        result = run_forecast(
            {"train_length": 3},
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
        model.fit.assert_called_once()
        model.predict.assert_called_once_with(
            n=2,
            past_covariates=None,
            future_covariates=covariates,
        )
        self.assertIs(result, forecast)
