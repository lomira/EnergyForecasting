import pandas as pd
from chronos import Chronos2Pipeline

from engine.forecaster.base_model import BaseEnergyModel


class ChronosModel(BaseEnergyModel):
    """Simple Chronos forecaster."""

    def __init__(self, model_config: dict):
        super().__init__(model_config)
        self.quantile_levels: list[float] = self.config.get(
            "quantile_levels", [0.1, 0.5, 0.9]
        )
        self.context_series: pd.Series | None = None
        self.model = None  # Placeholder for the actual Chronos model

    def fit(self, y: pd.Series, X: pd.DataFrame | None = None):
        self.model = Chronos2Pipeline.from_pretrained(
            "data/artefacts/chronos", device_map="cuda"
        )
        self.context_series = y.copy()
        return self

    def predict(
        self, horizon: int, X_future: pd.DataFrame | None = None
    ) -> pd.DataFrame:
        target_col = self.context_series.name or "target"
        context = self.context_series.rename(target_col).to_frame().assign(id=0)
        context["timestamp"] = context.index

        features = pd.DataFrame(index=X_future.index).assign(id=0)
        features["timestamp"] = features.index

        result = self.model.predict_df(
            context,
            future_df=features,
            prediction_length=len(features),  # Number of steps to forecast
            quantile_levels=self.quantile_levels,  # Quantile for probabilistic forecast
            id_column="id",  # Column identifying different time series
            timestamp_column="timestamp",  # Column with datetime information
            target=target_col,  # Column(s) with time series values to predict
        )
        result.index = X_future.index
        return result
