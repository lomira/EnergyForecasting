from typing import cast
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
from darts import TimeSeries

from engine.darts_pipeline.builder import build_data_transformers
from engine.series_utils import (
    covariates_time_series,
    forecast_covariates_time_series,
    get_load_ts,
)


class SeriesUtilsTests(TestCase):
    @patch("engine.series_utils.read_holidays")
    @patch("engine.series_utils.read_weather")
    def test_covariates_join_data_package_outputs(
        self, read_weather, read_holidays
    ) -> None:
        index = pd.date_range("2024-01-01", periods=3, freq="h", name="datetime")
        read_weather.return_value = pd.DataFrame(
            {
                "Alger_temperature_2m": [10.0, 11.0, 12.0],
                "unused": [1.0, 2.0, 3.0],
            },
            index=index,
        )
        read_holidays.return_value = pd.DataFrame(
            {"holidays": [True, False, False]}, index=index
        )

        series = covariates_time_series(index[0], index[-1])
        pipeline = build_data_transformers(
            {"feature_subset": ("holidays", "Alger_temperature_2m")}
        )["future_covariates"]
        data = cast(TimeSeries, pipeline.transform(series)).to_dataframe()

        self.assertEqual(
            list(series.components),
            ["Alger_temperature_2m", "unused", "holidays"],
        )
        self.assertEqual(list(data.columns), ["holidays", "Alger_temperature_2m"])
        self.assertEqual(data.iloc[0]["Alger_temperature_2m"], 10.0)
        self.assertEqual(data.iloc[1]["holidays"], 0.0)

    @patch("engine.series_utils.read_load")
    def test_load_series_uses_load_data_api(self, read_load) -> None:
        index = pd.date_range("2024-01-01", periods=2, freq="h", name="datetime")
        read_load.return_value = pd.DataFrame({"load_mw": [100.0, 110.0]}, index=index)

        series = get_load_ts(index[0], index[-1])

        self.assertEqual(len(series), 2)
        self.assertEqual(series.to_dataframe().iloc[-1]["load_mw"], 110.0)

    @patch("engine.series_utils.read_holidays")
    @patch("engine.series_utils.read_weather")
    def test_forecast_covariates_use_the_available_weather_vintage(
        self, read_weather, read_holidays
    ) -> None:
        index = pd.date_range("2024-01-01 23:00", periods=26, freq="h")
        read_weather.return_value = pd.DataFrame(
            {
                "Alger_temperature_2m": range(26),
                "Alger_temperature_2m_previous_day1": [101.0] * 26,
                "Alger_temperature_2m_previous_day2": [202.0] * 26,
            },
            index=index,
        )
        read_holidays.return_value = pd.DataFrame(
            {"holidays": [False] * 26}, index=index
        )

        series = forecast_covariates_time_series(
            index[0],
            index[-1],
            pd.Timestamp("2024-01-02 00:00"),
            feature_subset=("Alger_temperature_2m",),
        ).to_dataframe()

        self.assertEqual(series.iloc[0, 0], 0.0)
        self.assertEqual(series.iloc[1, 0], 101.0)
        self.assertEqual(series.iloc[-2, 0], 101.0)
        self.assertEqual(series.iloc[-1, 0], 202.0)

    @patch("engine.series_utils.read_holidays")
    @patch("engine.series_utils.read_weather")
    def test_forecast_covariates_reject_missing_weather_vintages(
        self, read_weather, read_holidays
    ) -> None:
        index = pd.date_range("2024-01-01", periods=2, freq="h")
        read_weather.return_value = pd.DataFrame(
            {"Alger_temperature_2m": [10.0, 11.0]}, index=index
        )
        read_holidays.return_value = pd.DataFrame(
            {"holidays": [False, False]}, index=index
        )

        with self.assertRaisesRegex(ValueError, "forecast-vintage column"):
            forecast_covariates_time_series(
                index[0],
                index[-1],
                index[-1],
                feature_subset=("Alger_temperature_2m",),
            )
