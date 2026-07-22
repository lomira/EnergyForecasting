"""Canonical backtest protocol.

One instance per comparison study, shared by ALL model configs.
Every metric row is stamped with ``spec_hash`` to guarantee fold-comparability across different models.
"""

import hashlib
import json
from dataclasses import dataclass
from typing import Literal

import pandas as pd
from darts import TimeSeries

TransformPolicy = Literal["per_retrain", "frozen_first_fold"]


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
    transform_policy : TransformPolicy
        ``"per_retrain"`` (default: native Darts — refit on every retrain).
        ``"frozen_first_fold"`` (fit pipelines once on first fold, reuse).
    """

    forecast_horizon: int
    stride: int
    train_length: int
    retrain: bool | int
    start: pd.Timestamp
    overlap_end: bool = False
    last_points_only: bool = True
    transform_policy: TransformPolicy = "per_retrain"

    def spec_hash(self) -> str:
        payload = {
            "h": self.forecast_horizon,
            "stride": self.stride,
            "train_length": self.train_length,
            "retrain": self.retrain,
            "start": str(self.start),
            "overlap_end": self.overlap_end,
            "lpo": self.last_points_only,
            "policy": self.transform_policy,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[
            :16
        ]

    def origin_grid(self, series: TimeSeries) -> list[pd.Timestamp]:
        """All forecast start-times. Origin o predicts steps o .. o+(h-1)."""
        freq = series.freq
        last_feasible = series.end_time() - (self.forecast_horizon - 1) * freq
        grid = list(pd.date_range(self.start, last_feasible, freq=self.stride * freq))
        if not grid:
            raise ValueError(
                "BacktestSpec produces zero origins — check start/horizon/series span."
            )
        return grid

    def first_train_slice(self, series: TimeSeries) -> TimeSeries:
        """Training window of the FIRST fold; used by the frozen_first_fold policy."""
        freq = series.freq
        end = self.start - freq  # last step strictly before first origin
        begin = end - (self.train_length - 1) * freq
        return series.slice(begin, end)
