"""Darts-native backtesting and one-shot forecasting.

Untransformed series go in; original-scale forecasts come out. Backtest results
are stamped with ``(spec_hash, config_hash, data_fp)`` for future persistence.
"""

import hashlib
from dataclasses import dataclass, field
from typing import Any, cast

import numpy as np
import pandas as pd
from darts import TimeSeries

from engine.darts_pipeline.builder import build_data_transformers, build_model
from engine.darts_pipeline.spec import BacktestSpec
from engine.logging_config import logger


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
    forecasts: TimeSeries
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


def _hf_kwargs(config: dict, spec: BacktestSpec) -> dict:
    return {
        "forecast_horizon": spec.forecast_horizon,
        "stride": spec.stride,
        "train_length": config["train_length"],
        "start": spec.start,
        "retrain": spec.retrain,
        "overlap_end": spec.overlap_end,
        "last_points_only": spec.last_points_only,
        "verbose": False,
    }


def run_backtest(
    config: dict,
    spec: BacktestSpec,
    series: TimeSeries,
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
    future_cov : TimeSeries, optional
        Future-covariate series.

    Returns
    -------
    BacktestResult
    """
    # ---- validation layer ----
    assert series.freq is not None, "series freq is None"
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
                config["train_length"],
                str(sorted(config.get("hyperparams", {}).items())),
            )
        ).encode()
    ).hexdigest()[:16]

    model = build_model(config)
    fc = model.historical_forecasts(
        series=series,
        future_covariates=future_cov,
        data_transformers=dt or None,
        **_hf_kwargs(config, spec),
    )
    if type(fc) is not TimeSeries:
        raise ValueError(
            "historical_forecasts returned non-TimeSeries (list of TimeSeries probably)"
        )

    scores = [wape(fc, series.slice_intersect(fc))]
    result = BacktestResult(
        forecasts=fc,
        fold_scores=scores,
        aggregate=float(np.nanmean(scores)),
        spec_hash=spec.spec_hash(),
        config_hash=config_hash,
        data_fp=fp,
    )
    fold_summary = ""
    if result.fold_scores:
        fold_summary = (
            f"\n  Fold scores:   min={min(result.fold_scores):.4f}, "
            f"max={max(result.fold_scores):.4f}, "
            f"median={sorted(result.fold_scores)[len(result.fold_scores) // 2]:.4f}"
        )
    logger.info(
        f"\n{'=' * 60}\n"
        f"  {model_name.removesuffix('Model')} backtest results\n"
        f"{'=' * 60}\n"
        f"  Origins:       {len(result.forecasts)}\n"
        f"  Aggregate WAPE: {result.aggregate:.4f} ({result.aggregate * 100:.2f}%)\n"
        f"  Spec hash:     {result.spec_hash}\n"
        f"  Config hash:   {result.config_hash}\n"
        f"  Data fp:       {result.data_fp}"
        f"{fold_summary}\n"
        f"{'=' * 60}"
    )
    return result


def run_forecast(
    config: dict,
    series: TimeSeries,
    horizon: int,
    *,
    future_cov: TimeSeries | None = None,
    future_scenario: TimeSeries | None = None,
) -> TimeSeries:
    """Fit a configured model on its training window and forecast ahead."""
    if horizon < 1:
        raise ValueError("horizon must be positive")

    train_length = config["train_length"]
    if len(series) < train_length:
        raise ValueError(
            f"series has {len(series)} steps; model requires {train_length}"
        )
    if future_cov is not None and future_cov.freq != series.freq:
        raise ValueError("future_cov freq mismatch")
    if future_scenario is not None:
        if future_cov is None:
            raise ValueError("future_scenario requires historical future_cov")
        if future_scenario.freq != series.freq:
            raise ValueError("future_scenario freq mismatch")

    training_series = series[-train_length:]
    transformers = build_data_transformers(config)

    target_transformer = transformers.get("series")
    transformed_series = (
        target_transformer.fit_transform(training_series)
        if target_transformer
        else training_series
    )

    future_transformer = transformers.get("future_covariates")
    if future_cov is not None and future_transformer is not None:
        future_transformer.fit(future_cov.slice_intersect(training_series))
        future_cov = cast(TimeSeries, future_transformer.transform(future_cov))
        if future_scenario is not None:
            future_scenario = cast(
                TimeSeries, future_transformer.transform(future_scenario)
            )

    if future_scenario is not None:
        assert future_cov is not None
        if not future_cov.components.equals(future_scenario.components):
            raise ValueError("future_scenario components must match future_cov")
        history = future_cov.to_dataframe()
        scenario = future_scenario.to_dataframe()
        future_cov = TimeSeries.from_dataframe(
            pd.concat(
                [history.loc[history.index < scenario.index[0]], scenario],
                axis=0,
            )
        )

    model = build_model(config)
    model.fit(
        transformed_series,
        future_covariates=future_cov,
    )
    forecast = cast(
        TimeSeries,
        model.predict(
            n=horizon,
            future_covariates=future_cov,
        ),
    )
    if target_transformer is not None:
        forecast = cast(
            TimeSeries,
            target_transformer.inverse_transform(
                forecast,
                insample=transformed_series,
            ),
        )
    return forecast
