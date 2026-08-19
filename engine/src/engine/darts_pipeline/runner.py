"""Darts-native backtesting and one-shot forecasting."""

from collections.abc import Sequence
from typing import cast

import numpy as np
import pandas as pd
from darts import TimeSeries
from darts.metrics import mape

from engine.darts_pipeline.builder import build_data_transformers, build_model
from engine.darts_pipeline.spec import BacktestSpec
from engine.logging_config import logger


def _select_covariates(
    covariates: TimeSeries | None, features: Sequence[str]
) -> TimeSeries | None:
    if covariates is None or not features:
        return None
    missing = set(features) - set(covariates.components)
    if missing:
        raise ValueError(f"Covariates missing configured components: {sorted(missing)}")
    return covariates[list(features)]


def _period_metrics(
    series: TimeSeries, forecasts: TimeSeries, frequency: str
) -> dict[str, float]:
    peaks = pd.concat(
        {
            "actual": series.to_series().resample(frequency).max(),
            "forecast": forecasts.to_series().resample(frequency).max(),
        },
        axis=1,
        join="inner",
    ).dropna()
    errors = peaks["forecast"] - peaks["actual"]
    return {
        "mae": float(errors.abs().mean()),
        "wape": float(errors.abs().sum() / peaks["actual"].abs().sum() * 100),
        "bias": float(errors.mean()),
    }


def _metrics(series: TimeSeries, forecasts: TimeSeries) -> dict[str, float]:
    """Compute load-curve, daily-peak, and monthly-peak metrics."""
    hourly = _period_metrics(series, forecasts, "h")
    daily = _period_metrics(series, forecasts, "D")
    monthly = _period_metrics(series, forecasts, "MS")
    return {
        **{f"hourly_{name}": value for name, value in hourly.items()},
        "hourly_mape": float(np.asarray(mape(series, forecasts)).item()),
        **{f"daily_peak_{name}": value for name, value in daily.items()},
        **{f"monthly_peak_{name}": value for name, value in monthly.items()},
    }


def _log_metrics(label: str, scores: dict[str, float]) -> None:
    logger.info(
        "{} | Hourly MAE: {hourly_mae:.2f} MW | "
        "WAPE: {hourly_wape:.2f}% | MAPE: {hourly_mape:.2f}% | "
        "Bias: {hourly_bias:.2f} MW | "
        "Daily peak MAE: {daily_peak_mae:.2f} MW | "
        "WAPE: {daily_peak_wape:.2f}% | Bias: {daily_peak_bias:.2f} MW | "
        "Monthly peak MAE: {monthly_peak_mae:.2f} MW | "
        "WAPE: {monthly_peak_wape:.2f}% | Bias: {monthly_peak_bias:.2f} MW",
        label,
        **scores,
    )


def run_backtest(
    config: dict,
    spec: BacktestSpec,
    series: TimeSeries,
    future_cov: TimeSeries | None = None,
) -> TimeSeries:
    """Run a Darts-native backtest for a model config."""
    # ---- validation layer ----
    assert series.freq is not None, "series freq is None"
    model = build_model(config)
    features = config.get("feature_subset", ())
    if features and not model.supports_future_covariates:
        raise ValueError(
            f"{type(model).__name__} does not support future covariates, "
            "but feature_subset is not empty"
        )
    future_cov = _select_covariates(future_cov, features)
    if future_cov is not None:
        assert future_cov.freq == series.freq, "future_cov freq mismatch"

    dt = build_data_transformers(config)

    fc = model.historical_forecasts(
        series=series,
        future_covariates=future_cov,
        data_transformers=dt or None,
        forecast_horizon=spec.forecast_horizon,
        stride=spec.stride,
        train_length=config["train_length"],
        start=spec.start,
        retrain=spec.retrain,
        overlap_end=spec.overlap_end,
        last_points_only=spec.last_points_only,
        verbose=False,
    )
    fc = cast(TimeSeries, fc)
    label = f"{type(model).__name__} backtest"
    _log_metrics(label, _metrics(series, fc))

    return fc


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

    model = build_model(config)
    features = config.get("feature_subset", ())
    if features and not model.supports_future_covariates:
        raise ValueError(
            f"{type(model).__name__} does not support future covariates, "
            "but feature_subset is not empty"
        )
    future_cov = _select_covariates(future_cov, features)
    future_scenario = _select_covariates(future_scenario, features)

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
