from unittest import TestCase
from unittest.mock import patch

import pandas as pd
from engine.series_utils import covariates_time_series, load_time_series


class SeriesUtilsTests(TestCase):
    @patch("engine.series_utils.read_holidays")
    @patch("engine.series_utils.read_weather")
    def test_covariates_join_data_package_outputs(
        self, read_weather, read_holidays
    ) -> None:
        index = pd.date_range(
            "2024-01-01", periods=3, freq="h", name="datetime"
        )
        read_weather.return_value = pd.DataFrame(
            {"Alger_temperature_2m": [10.0, 11.0, 12.0]}, index=index
        )
        read_holidays.return_value = pd.DataFrame(
            {"holidays": [True, False, False]}, index=index
        )

        series = covariates_time_series(
            index[0],
            index[-1],
            feature_subset=("Alger_temperature_2m", "holidays"),
        )
        data = series.to_dataframe()

        self.assertEqual(
            list(data.columns), ["Alger_temperature_2m", "holidays"]
        )
        self.assertEqual(data.iloc[0]["Alger_temperature_2m"], 10.0)
        self.assertEqual(data.iloc[1]["holidays"], 0.0)

    @patch("engine.series_utils.read_load")
    def test_load_series_uses_load_data_api(self, read_load) -> None:
        index = pd.date_range("2024-01-01", periods=2, freq="h", name="datetime")
        read_load.return_value = pd.DataFrame({"load_mw": [100.0, 110.0]}, index=index)

        series = load_time_series(index[0], index[-1])

        self.assertEqual(len(series), 2)
        self.assertEqual(series.to_dataframe().iloc[-1]["load_mw"], 110.0)
