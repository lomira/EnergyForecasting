"""Canonical backtest protocol.

One instance per comparison study, shared by ALL model configs.
Every metric row is stamped with ``spec_hash`` to guarantee fold-comparability across different models.
"""

import hashlib
import json
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class BacktestSpec:
    """Canonical, hashed backtest protocol.

    Parameters
    ----------
    forecast_horizon : int
        Steps ahead to forecast (e.g. 24 for day-ahead hourly).
    stride : int
        Steps between successive forecast origins.
    train_length : int
        Rolling window length in steps. Caps per-fold fit cost.
    retrain : bool | int
        True = every origin; int k = every k-th origin.
    start : pd.Timestamp
        First forecast origin — EXPLICIT so every model shares the same grid.
    overlap_end : bool
        Passed through to Darts ``historical_forecasts``.
    last_points_only : bool
        Passed through to Darts ``historical_forecasts``.
    """

    forecast_horizon: int
    stride: int
    train_length: int
    retrain: bool | int
    start: pd.Timestamp
    overlap_end: bool = False
    last_points_only: bool = True

    def spec_hash(self) -> str:
        payload = {
            "h": self.forecast_horizon,
            "stride": self.stride,
            "train_length": self.train_length,
            "retrain": self.retrain,
            "start": str(self.start),
            "overlap_end": self.overlap_end,
            "lpo": self.last_points_only,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[
            :16
        ]
