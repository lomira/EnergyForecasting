"""Canonical backtest protocol shared by all model configs."""

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class BacktestSpec:
    """Canonical backtest protocol.

    Parameters
    ----------
    forecast_horizon : int
        Steps ahead to forecast (e.g. 24 for day-ahead hourly).
    stride : int
        Steps between successive forecast origins.
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
    retrain: bool | int
    start: pd.Timestamp
    overlap_end: bool = False
    last_points_only: bool = True
