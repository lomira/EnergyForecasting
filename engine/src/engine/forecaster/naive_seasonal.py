import pickle

import numpy as np
import pandas as pd

from engine.forecaster.base_model import BaseEnergyModel


class NaiveSeasonalModel(BaseEnergyModel):
    """Simple seasonal naive forecaster.

    This model repeats the last ``k`` values from the training set to create a forecast for any horizon.
    """

    def __init__(self, model_config: dict):
        super().__init__(model_config)
        self.k: int = self.config.get("k")
        self.last_values: list[float] | None = None

    def fit(self, y: pd.Series, X: pd.DataFrame | None = None):
        """Store the last ``k`` values from the training series."""
        values = y.astype(float).tolist()
        self.last_values = values[-self.k :] if self.k > 0 else []
        return self

    def predict(
        self, horizon: int, X_future: pd.DataFrame | None = None
    ) -> pd.DataFrame:
        """Generate a forecast by repeating the stored seasonal pattern."""
        if self.last_values is None:
            raise ValueError("The model must be fitted before calling predict().")

        if horizon <= 0:
            return pd.DataFrame({"forecast": []})

        if not self.last_values:
            raise ValueError("No values available to forecast.")

        period = len(self.last_values)
        repeated = np.tile(self.last_values, horizon // period + 1)[:horizon]
        return pd.DataFrame({"forecast": repeated})

    def save(self, path: str):
        """Persist the fitted model to disk."""
        with open(path, "wb") as handle:
            pickle.dump(self, handle)

    @classmethod
    def load(cls, path: str) -> NaiveSeasonalModel:
        """Load a fitted model from disk."""
        with open(path, "rb") as handle:
            return pickle.load(handle)
