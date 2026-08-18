"""Darts-native backtesting and one-shot forecasting."""

from collections.abc import Sequence
from typing import cast

import pandas as pd
from darts import TimeSeries
from darts.metrics import wmape

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
    if type(fc) is not TimeSeries:
        raise ValueError(
            "historical_forecasts returned non-TimeSeries (list of TimeSeries probably)"
        )

    score = cast(float, wmape(series, fc)) / 100
    logger.info(
        f"{type(model).__name__.removesuffix('Model')} backtest WAPE: "
        f"{score:.4f} ({score:.2%})"
    )
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
