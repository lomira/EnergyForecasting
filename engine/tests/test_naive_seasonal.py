import unittest
from pathlib import Path

import pandas as pd
import pandas.testing as pdt
from engine.forecaster.naive_seasonal import NaiveSeasonalModel


class NaiveSeasonalModelTests(unittest.TestCase):
    def test_fit_and_predict_repeat_last_k_values(self) -> None:
        model = NaiveSeasonalModel({"k": 2})
        # Create a series with 3 dates in index and 3 values
        train_series = pd.Series(
            [10.0, 20, 30], index=pd.date_range("2023-01-01", periods=3), name="target"
        )

        model.fit(train_series)
        predictions = model.predict(horizon=4)

        expected_results = pd.DataFrame(
            {"forecast": [20.0, 30, 20, 30]},
            index=pd.RangeIndex(4),
        )
        pdt.assert_frame_equal(predictions, expected_results)

    def test_save_and_load_roundtrip(self) -> None:
        model = NaiveSeasonalModel({"k": 1})
        train_series = pd.Series(
            [5, 6, 7], index=pd.date_range("2023-01-01", periods=3), name="target"
        )
        model.fit(train_series)

        path = Path("/tmp/naive_seasonal_model.pkl")
        model.save(str(path))
        loaded = NaiveSeasonalModel.load(str(path))

        self.assertEqual(loaded.last_values, [7])
        self.assertEqual(
            loaded.predict(horizon=2)["forecast"].tolist(),
            [7, 7],
        )

    def test_backtest_rolling_origin(self) -> None:
        model = NaiveSeasonalModel({"k": 1})
        # Hourly series; seasonal-naive with k=1 repeats the last value.
        idx = pd.date_range("2023-01-01", periods=12, freq="h")
        y = pd.Series(range(12), index=idx, name="target")

        # Positions 3..9 (inclusive) are the origins; horizon=2 (forecast ends at 11).
        result = model.backtest(
            y,
            start=3,
            end=9,
            stride=2,
            retrain_freq=1,
            horizon=2,
        )

        self.assertIn("timestamp", result.columns)
        self.assertIn("origin", result.columns)
        self.assertIn("train_origin", result.columns)
        self.assertIn("actual", result.columns)
        self.assertIn("predicted", result.columns)
        # k=1 naive repeats the value at the most recent train_origin for every step.
        for _, row in result.iterrows():
            train_value = y.asof(row["train_origin"])
            self.assertEqual(row["predicted"], train_value)
            # With retrain_freq=1 the model is refit at every origin.
            self.assertEqual(row["train_origin"], row["origin"])

    def test_backtest_stride_and_retrain_freq(self) -> None:
        model = NaiveSeasonalModel({"k": 1})
        idx = pd.date_range("2023-01-01", periods=12, freq="h")
        y = pd.Series(range(12), index=idx, name="target")

        # stride=1 (overlapping windows), retrain_freq=3 (refit every 3rd origin).
        result = model.backtest(
            y,
            start=3,
            end=9,
            stride=1,
            retrain_freq=3,
            horizon=2,
        )

        # Distinct origins step by stride=1: positions 3..9 = 7 origins.
        origins = result["origin"].drop_duplicates().tolist()
        self.assertEqual(len(origins), 7)
        # train_origin only changes every retrain_freq=3 origins (positions 3, 6, 9).
        train_origins = result["train_origin"].drop_duplicates().tolist()
        self.assertEqual(len(train_origins), 3)


if __name__ == "__main__":
    unittest.main()
