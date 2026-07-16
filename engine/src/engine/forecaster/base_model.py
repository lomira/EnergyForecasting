from abc import ABC, abstractmethod

import pandas as pd


class BaseEnergyModel(ABC):
    def __init__(self, model_config: dict):
        self.config = model_config

    @abstractmethod
    def fit(self, train_df: pd.DataFrame, target_col: str):
        """Train the model on historical data."""
        pass

    @abstractmethod
    def predict(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """Generate a forecast."""
        pass

    def backtest(
        self,
        data: pd.DataFrame,
        target_col: str,
        start_date: str,
        end_date: str,
        retrain_freq: int,
    ) -> pd.DataFrame:
        """
        Generic backtesting logic that works for ANY model that implements fit and predict
        """
        pass

    @abstractmethod
    def save(self, path: str):
        pass

    @abstractmethod
    def load(self, path: str):
        pass
