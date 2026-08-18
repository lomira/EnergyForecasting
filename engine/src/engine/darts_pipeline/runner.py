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
                config["train_length"],
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
    past_cov: TimeSeries | None = None,
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
    if past_cov is not None and past_cov.freq != series.freq:
        raise ValueError("past_cov freq mismatch")
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

    transformed_covariates: dict[str, TimeSeries | None] = {}
    for name, covariates in (
        ("past_covariates", past_cov),
        ("future_covariates", future_cov),
    ):
        transformer = transformers.get(name)
        if covariates is not None and transformer is not None:
            transformer.fit(covariates.slice_intersect(training_series))
            covariates = cast(TimeSeries, transformer.transform(covariates))
        transformed_covariates[name] = covariates

    model = build_model(config)
    model.fit(
        transformed_series,
        past_covariates=transformed_covariates["past_covariates"],
        future_covariates=transformed_covariates["future_covariates"],
    )
    forecast = cast(
        TimeSeries,
        model.predict(
            n=horizon,
            past_covariates=transformed_covariates["past_covariates"],
            future_covariates=transformed_covariates["future_covariates"],
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
