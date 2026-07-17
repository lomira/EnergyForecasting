import unittest

import pandas as pd
import pandas.testing as pdt
from engine.forecaster.chronos import ChronosModel


class ChronosModelTests(unittest.TestCase):
    def test_fit_and_predict_df(self) -> None:
        model = ChronosModel({"quantile_levels": [0.5]})
        train_series = pd.Series(
            [1.0, 2.0, 3.0],
            index=pd.date_range("2023-01-01", periods=3),
            name="target",
        )

        model.fit(train_series)
        future_idx = pd.date_range("2023-01-04", periods=2)
        predictions = model.predict(horizon=2, X_future=pd.DataFrame(index=future_idx))

        # Verify predictions have correct shape and index
        self.assertEqual(len(predictions), 2)
        pdt.assert_index_equal(predictions.index, future_idx)
        # Chronos outputs 'predictions' and quantile columns
        self.assertIn("predictions", predictions.columns)
        self.assertIn("0.5", predictions.columns)


if __name__ == "__main__":
    unittest.main()
