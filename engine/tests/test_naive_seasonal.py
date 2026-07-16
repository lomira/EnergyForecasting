import unittest
from pathlib import Path

import pandas as pd
import pandas.testing as pdt
from engine.forecaster.naive_seasonal import NaiveSeasonalModel


class NaiveSeasonalModelTests(unittest.TestCase):
    def test_fit_and_predict_repeat_last_k_values(self) -> None:
        model = NaiveSeasonalModel({"k": 2})
        # Create a df with 3 dates in index and a target column with 3 values
        train_df = pd.DataFrame(
            {"target": [10.0, 20, 30]}, index=pd.date_range("2023-01-01", periods=3)
        )

        model.fit(train_df)
        predict_df = pd.DataFrame(index=pd.date_range("2023-01-04", periods=4))
        predictions = model.predict(predict_df)

        expected_results = pd.DataFrame(
            {"forecast": [20.0, 30, 20, 30]},
            index=pd.date_range("2023-01-04", periods=4),
        )
        pdt.assert_frame_equal(predictions, expected_results)

    def test_save_and_load_roundtrip(self) -> None:
        model = NaiveSeasonalModel({"k": 1})
        train_df = pd.DataFrame(
            {"target": [5, 6, 7]}, index=pd.date_range("2023-01-01", periods=3)
        )
        model.fit(train_df)

        path = Path("/tmp/naive_seasonal_model.pkl")
        model.save(str(path))
        loaded = NaiveSeasonalModel.load(str(path))

        self.assertEqual(loaded.last_values, [7])
        self.assertEqual(
            loaded.predict(pd.DataFrame(index=range(2)))["forecast"].tolist(),
            [7, 7],
        )


if __name__ == "__main__":
    unittest.main()
