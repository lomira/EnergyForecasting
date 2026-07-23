"""THE backtest path using Darts native ``historical_forecasts``.

Untransformed series go in; original-scale forecasts come out.
Every result is stamped with ``(spec_hash, config_hash, data_fp)`` for future save load efficienty
"""

import hashlib
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from darts import TimeSeries

from engine.darts_pipeline.builder import build_data_transformers, build_model
from engine.darts_pipeline.spec import BacktestSpec


def data_fingerprint(series: TimeSeries, decimals: int = 2) -> str:
    """Fingerprint of metadata + rounded values (robust to float repr noise)."""
    df = series.to_dataframe()
    # Round numeric values to a safe decimal place to avoid float noise
    df_rounded = df.round(decimals)
    payload = {
        "freq": str(series.freq),
        "start": str(series.start_time()),
        "end": str(series.end_time()),
        "len": len(series),
        "components": list(series.components),
        "dtypes": [str(d) for d in df.dtypes],
    }

    hasher = hashlib.sha256()

    # Hash metadata
    meta_bytes = pd.util.hash_pandas_object(
        pd.Series([str(v) for v in payload.values()]), index=True
    ).values.tobytes()
    hasher.update(meta_bytes)
    # Hash data
    values_bytes = pd.util.hash_pandas_object(df_rounded, index=True).values.tobytes()
    hasher.update(values_bytes)

    return hasher.hexdigest()[:16]


@dataclass
class BacktestResult:
    forecasts: list[TimeSeries]
    fold_scores: list[float]
    aggregate: float  # mean of per-fold WAPE
    spec_hash: str
    config_hash: str
    data_fp: str
    metadata: dict[str, Any] = field(default_factory=dict)


def wape(forecast: TimeSeries, actual: TimeSeries) -> float:
    """Weighted Absolute Percentage Error: sum|e| / sum|y|."""
    f, a = forecast.all_values(), actual.all_values()
    denom = np.abs(a).sum()
    if denom == 0:
        return np.nan
    return float(np.abs(f - a).sum() / denom)


def _hf_kwargs(spec: BacktestSpec) -> dict:
    return dict(
        forecast_horizon=spec.forecast_horizon,
        stride=spec.stride,
        train_length=spec.train_length,
        start=spec.start,
        retrain=spec.retrain,
        overlap_end=spec.overlap_end,
        last_points_only=spec.last_points_only,
        verbose=False,
    )


def run_backtest(
    config: dict,
    spec: BacktestSpec,
    series: TimeSeries,
    past_cov: TimeSeries | None = None,
    future_cov: TimeSeries | None = None,
) -> BacktestResult:
    """Run a Darts-native backtest for a model config.

    Parameters
    ----------
    config : dict
        Model configuration dict (from ``models/`` package).
    spec : BacktestSpec
        Evaluation protocol.
    series : TimeSeries
        Target series.
    past_cov, future_cov : TimeSeries, optional
        Covariate series.

    Returns
    -------
    BacktestResult
    """
    # ---- validation layer ----
    assert series.freq is not None, "series freq is None"
    if past_cov is not None:
        assert past_cov.freq == series.freq, "past_cov freq mismatch"
    if future_cov is not None:
        assert future_cov.freq == series.freq, "future_cov freq mismatch"

    dt = build_data_transformers(config)
    fp = data_fingerprint(series)

    # ---- compute config hash ----
    model_cls = config["model_cls"]
    model_name = (
        model_cls.__name__ if hasattr(model_cls, "__name__") else str(model_cls)
    )
    config_hash = hashlib.sha256(
        str(
            (
                config["name"],
                model_name,
                str(sorted(config.get("hyperparams", {}).items())),
            )
        ).encode()
    ).hexdigest()[:16]

    model = build_model(config)
    fc = model.historical_forecasts(
        series=series,
        past_covariates=past_cov,
        future_covariates=future_cov,
        data_transformers=dt or None,
        **_hf_kwargs(spec),
    )
    forecasts = fc if isinstance(fc, list) else [fc]

    scores = [wape(f, series.slice_intersect(f)) for f in forecasts]
    return BacktestResult(
        forecasts=forecasts,
        fold_scores=scores,
        aggregate=float(np.nanmean(scores)),
        spec_hash=spec.spec_hash(),
        config_hash=config_hash,
        data_fp=fp,
    )
