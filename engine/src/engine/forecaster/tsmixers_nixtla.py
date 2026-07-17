import pandas as pd
from neuralforecast import NeuralForecast
from neuralforecast.models import TSMixerx

from engine.forecaster.base_model import BaseEnergyModel


class TSMixer(BaseEnergyModel):
    """Simple implementation of TSMixerx using the Nixtla implementation."""

    def __init__(self, model_config: dict):
        super().__init__(model_config)
        self.freq = self.config.get("freq", "h")
        # `freq` is a NeuralForecast arg, not a TSMixerx arg; pass the rest through.
        model_kwargs = {k: v for k, v in self.config.items() if k != "freq"}
        # Disable the default TensorBoard logger so no lightning_logs are written.
        model_kwargs.setdefault("logger", False)
        self.model = NeuralForecast(models=[TSMixerx(**model_kwargs)], freq=self.freq)
        self.futr_exog_list = None

    def fit(self, y: pd.Series, X: pd.DataFrame | None = None):
        # All covariates available at fit time are future exogenous.
        if X is not None:
            self.futr_exog_list = list(X.columns)
            self.model.models[0].futr_exog_list = self.futr_exog_list

        df = y.rename("y").to_frame().reset_index()
        df.columns = ["ds", "y"]
        df["unique_id"] = "0"
        if X is not None:
            df = df.merge(X, left_on="ds", right_index=True)

        self.model.fit(df=df)
        return self

    def predict(
        self, horizon: int, X_future: pd.DataFrame | None = None
    ) -> pd.DataFrame:
        futr_df = None
        if X_future is not None and self.futr_exog_list:
            futr_df = X_future.reset_index()
            futr_df.columns = ["ds"] + list(X_future.columns)
            futr_df["unique_id"] = "0"

        fcst = self.model.predict(futr_df=futr_df)
        fcst = fcst[fcst["unique_id"] == "0"].drop(columns=["unique_id"])
        fcst = fcst.set_index("ds")
        return fcst
