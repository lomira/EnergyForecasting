import pickle

import pandas as pd
from chronos import Chronos2Pipeline

from engine.forecaster.base_model import BaseEnergyModel


class ChronosModel(BaseEnergyModel):
    """Simple Chronos forecaster."""

    def __init__(self, model_config: dict):
        super().__init__(model_config)
        self.quantile_levels: list[float] = model_config.get(
            "quantile_levels", [0.1, 0.5, 0.9]
        )
        self.context_df: pd.DataFrame | None = None
        self.model = None  # Placeholder for the actual Chronos model

    def fit(self, target_df: pd.DataFrame):
        self.model = Chronos2Pipeline.from_pretrained(
            "amazon/chronos-2", device_map="cuda"
        )
        self.context_df = target_df.copy()
        return self

    def predict(self, features_df: pd.DataFrame) -> pd.DataFrame:
        if self.context_df is None or self.model is None:
            raise ValueError("The model must be fitted before calling predict().")

        context = self.context_df.copy()
        features = features_df.copy()

        context["id"] = 0
        context["timestamp"] = context.index
        features["id"] = 0
        features["timestamp"] = features.index
        target_col = context.columns[0]  # Assuming the first column is the target
        """Generate a forecast."""
        return self.model.predict_df(
            context,
            future_df=features,
            prediction_length=len(features),  # Number of steps to forecast
            quantile_levels=self.quantile_levels,  # Quantile for probabilistic forecast
            id_column="id",  # Column identifying different time series
            timestamp_column="timestamp",  # Column with datetime information
            target=target_col,  # Column(s) with time series values to predict
        )

    def save(self, path: str):
        """Persist the fitted model to disk."""
        model = self.model
        self.model = None
        try:
            with open(path, "wb") as handle:
                pickle.dump(self, handle)
        finally:
            self.model = model

    @classmethod
    def load(cls, path: str) -> ChronosModel:
        """Load a fitted model from disk."""
        with open(path, "rb") as handle:
            model = pickle.load(handle)
        if model.context_df is not None and model.model is None:
            model.model = Chronos2Pipeline.from_pretrained(
                "amazon/chronos-2", device_map="cpu"
            )
        return model
