import unittest

import pandas as pd
import pandas.testing as pdt
from engine.forecaster.tsmixers_nixtla import TSMixer


class TSMixerTests(unittest.TestCase):
    def test_fit_and_predict_shape_and_index(self) -> None:
        model = TSMixer(
            {"h": 2, "input_size": 3, "n_series": 1, "max_steps": 1, "logger": False}
        )
        train_series = pd.Series(
            list(range(10)),
            index=pd.date_range("2023-01-01", periods=10, freq="h"),
            name="target",
        )

        model.fit(train_series)
        future_idx = pd.date_range("2023-01-01 10:00", periods=2, freq="h")
        predictions = model.predict(horizon=2, X_future=pd.DataFrame(index=future_idx))

        # Verify predictions have correct shape and index
        self.assertEqual(len(predictions), 2)
        pdt.assert_index_equal(predictions.index, future_idx, check_names=False)
        # TSMixerx outputs a single forecast column named after the model
        self.assertIn("TSMixerx", predictions.columns)

    def test_fit_and_predict_with_covariates(self) -> None:
        model = TSMixer(
            {
                "h": 2,
                "input_size": 3,
                "n_series": 1,
                "max_steps": 1,
                "logger": False,
            }
        )
        idx = pd.date_range("2023-01-01", periods=10, freq="h")
        train_series = pd.Series(list(range(10)), index=idx, name="target")
        X = pd.DataFrame({"temp": list(range(10))}, index=idx)

        model.fit(train_series, X)
        # futr_exog_list is derived from X at fit time
        self.assertEqual(model.futr_exog_list, ["temp"])

        future_idx = pd.date_range("2023-01-01 10:00", periods=2, freq="h")
        X_future = pd.DataFrame({"temp": [40.0, 50.0]}, index=future_idx)
        predictions = model.predict(horizon=2, X_future=X_future)

        self.assertEqual(len(predictions), 2)
        pdt.assert_index_equal(predictions.index, future_idx, check_names=False)
        self.assertIn("TSMixerx", predictions.columns)


if __name__ == "__main__":
    unittest.main()
