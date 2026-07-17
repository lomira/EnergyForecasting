from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd


class BaseEnergyModel(ABC):
    """Minimal univariate forecaster contract."""

    def __init__(self, model_config: dict):
        self.config = model_config

    @abstractmethod
    def fit(self, y: pd.Series, X: Optional[pd.DataFrame] = None) -> "BaseEnergyModel":
        """Fit on the univariate target ``y`` with optional covariates ``X``.

        In this first implementation X is only Future (Variables known at prediction time)
        """
        pass

    @abstractmethod
    def predict(
        self, horizon: int, X_future: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """Forecast horizon steps ahead using the covariates in X_future."""
        pass

    def get_params(self) -> dict:
        """Return a copy of ``self.config`` (sklearn-style)."""
        return copy.deepcopy(self.config)

    def set_params(self, **params) -> "BaseEnergyModel":
        """Update ``self.config`` in place (sklearn-style)."""
        self.config.update(params)
        return self

    def clone(self) -> "BaseEnergyModel":
        """Return a fresh, unfitted copy of this model with the same config."""
        return type(self)(self.get_params(deep=False))

    def backtest(
        self,
        y: pd.Series,
        X: Optional[pd.DataFrame] = None,
        *,
        start: int,
        end: int,
        horizon: int = 24,
        stride: int = 1,
        retrain_freq: int = 1,
    ) -> pd.DataFrame:
        """Rolling-origin (walk-forward) backtest that works for ANY model.

        Everything is expressed in **step positions**
        Windows slide by ``stride`` steps; the model is refit every ``retrain_freq`` windows

        Parameters
        ----------
        y : pd.Series
            Full target history, indexed by timestamp.
        X : pd.DataFrame, optional
            Full covariate frame aligned to ``y`` (past) and to the forecast
            window (future). Split internally per origin.
        start, end : int
            Step positions (0-based) bounding the backtest window. ``start`` is
            the first origin; ``end`` is the last origin (inclusive). Must
            satisfy ``0 <= start <= end < len(y) - horizon``.
        horizon : int
            Forecast horizon (steps) per origin.
        stride : int
            Number of steps the forecast window slides between successive
            origins. ``stride < horizon`` -> overlapping windows;
            ``stride == horizon`` -> contiguous windows; ``stride > horizon`` -> gaps.
        retrain_freq : int
            Refit the model every ``retrain_freq`` origins (windows). Defaults
            to 1 (refit at every origin). Set higher to simulate periodic
            retraining with a stale model between refits.
        """
        if stride < 1:
            raise ValueError("stride must be >= 1")
        if retrain_freq < 1:
            raise ValueError("retrain_freq must be >= 1")
        if horizon < 1:
            raise ValueError("horizon must be >= 1")

        # we can assume y and x are sorted
        n = len(y)
        idx = y.index
        if not (0 <= start <= end < n - horizon):
            raise ValueError(
                f"require 0 <= start <= end < len(y)-horizon "
                f"(got start={start}, end={end}, len(y)={n}, horizon={horizon})"
            )

        # Build a dictionnary where the key is the origin when the refit and the value are all the predict done on this refit.
        all_origins = list(range(start, end + 1, stride))
        refit_origins = [
            pos for i, pos in enumerate(all_origins) if i % retrain_freq == 0
        ]
        # Group each predict origin under the refit origin that owns it.
        refit_to_predicts: dict[int, list[int]] = {r: [r] for r in refit_origins}
        for pos in all_origins:
            if pos in refit_to_predicts:
                continue  # already the refit origin itself
            owner = max(r for r in refit_origins if r < pos)
            refit_to_predicts[owner].append(pos)

        rows: list[dict] = []

        # Walk over the key => refit, walk over the value of each key => predict.
        for train_pos, predict_origins in refit_to_predicts.items():
            # Fit on all history
            y_train = y.iloc[: train_pos + 1]
            X_train = X.iloc[: train_pos + 1] if X is not None else None
            self.fit(y_train, X_train)

            # Predict for each prediction point
            for origin_pos in predict_origins:
                fc_idx = idx[origin_pos + 1 : origin_pos + horizon + 1]
                X_future = X.loc[fc_idx] if X is not None else None

                preds = self.predict(horizon=horizon, X_future=X_future)
                pred_col = preds.columns[0]
                pred_series = preds[pred_col].set_axis(fc_idx)

                actuals = y.loc[fc_idx]
                for ts in fc_idx:
                    rows.append(
                        {
                            "timestamp": ts,
                            "origin": idx[origin_pos],
                            "train_origin": idx[train_pos],
                            "actual": actuals.get(ts, pd.NA),
                            "predicted": pred_series.get(ts, pd.NA),
                        }
                    )

        return pd.DataFrame(
            rows,
            columns=["timestamp", "origin", "train_origin", "actual", "predicted"],
        )
