"""THE backtest path using Darts native ``historical_forecasts``.

Untransformed series go in; original-scale forecasts come out.
Every result is stamped with ``(spec_hash, config_hash, data_fp)`` for future save load efficienty
"""

import hashlib
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np
import pandas as pd
from darts import TimeSeries

from engine.darts_pipeline.builder import build_data_transformers, build_model
from engine.darts_pipeline.spec import BacktestSpec

FoldCallback = Callable[[int, float], None]  # (fold_idx, fold_wape) -> None


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
    *,
    on_fold: FoldCallback | None = None,
    chunk_size: int = 1,
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
    on_fold : FoldCallback, optional
        Called after each fold with ``(fold_idx, wape)`` — used for Optuna pruning.
    chunk_size : int
        Number of origins per chunk for chunked execution (stage-1 approximation).

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

    if spec.transform_policy == "frozen_first_fold":
        forecasts = _run_frozen(config, spec, series, past_cov, future_cov, dt)
    elif on_fold is None:
        forecasts = _run_native(config, spec, series, past_cov, future_cov, dt)
    else:
        forecasts = _run_chunked(
            config,
            spec,
            series,
            past_cov,
            future_cov,
            dt,
            on_fold=on_fold,
            chunk_size=chunk_size,
        )

    scores = [wape(f, series.slice_intersect(f)) for f in forecasts]
    return BacktestResult(
        forecasts=forecasts,
        fold_scores=scores,
        aggregate=float(np.nanmean(scores)),
        spec_hash=spec.spec_hash(),
        config_hash=config_hash,
        data_fp=fp,
    )


def _run_native(config, spec, series, past_cov, future_cov, dt) -> list[TimeSeries]:
    """Single native call — the default path."""
    model = build_model(config)
    fc = model.historical_forecasts(
        series=series,
        past_covariates=past_cov,
        future_covariates=future_cov,
        data_transformers=dt or None,
        **_hf_kwargs(spec),
    )
    return fc if isinstance(fc, list) else [fc]


def _run_chunked(
    config,
    spec,
    series,
    past_cov,
    future_cov,
    dt,
    *,
    on_fold: FoldCallback,
    chunk_size: int,
) -> list[TimeSeries]:
    """Optuna path — execute origin-chunks so intermediate scores can be reported."""
    grid = spec.origin_grid(series)
    forecasts: list[TimeSeries] = []
    for start in range(0, len(grid), chunk_size):
        chunk = grid[start : start + chunk_size]
        model = build_model(config)
        chunk_series = series.drop_after(chunk[-1])
        fc = model.historical_forecasts(
            series=chunk_series,
            past_covariates=past_cov,
            future_covariates=future_cov,
            data_transformers=dt or None,
            **{**_hf_kwargs(spec), "start": chunk[0], "retrain": True},
        )
        fc = fc if isinstance(fc, list) else [fc]
        for f in fc:
            forecasts.append(f)
            on_fold(len(forecasts) - 1, wape(f, series.slice_intersect(f)))
    return forecasts


def _run_frozen(config, spec, series, past_cov, future_cov, dt) -> list[TimeSeries]:
    """Cheap leakage-free mode: fit pipelines ONCE on the first fold's train slice."""
    target_pipe = dt.get("series")
    if target_pipe is not None:
        first_train = spec.first_train_slice(series)
        target_pipe.fit(first_train)
        series_t = target_pipe.transform(series)
    else:
        series_t = series

    past_t = None
    if past_cov is not None and "past_covariates" in dt:
        past_pipe = dt["past_covariates"]
        past_pipe.fit(
            past_cov.slice(
                spec.start - (spec.train_length - 1) * series.freq,
                spec.start - series.freq,
            )
        )
        past_t = past_pipe.transform(past_cov)

    fut_t = None
    if future_cov is not None and "future_covariates" in dt:
        fut_pipe = dt["future_covariates"]
        fut_pipe.fit(
            future_cov.slice(
                spec.start - (spec.train_length - 1) * series.freq,
                spec.start - series.freq,
            )
        )
        fut_t = fut_pipe.transform(future_cov)

    model = build_model(config)
    fc = model.historical_forecasts(
        series=series_t,
        past_covariates=past_t,
        future_covariates=fut_t,
        **_hf_kwargs(spec),
    )
    fc = fc if isinstance(fc, list) else [fc]

    if target_pipe is not None:
        fc = [target_pipe.inverse_transform(f) for f in fc]
    return fc
