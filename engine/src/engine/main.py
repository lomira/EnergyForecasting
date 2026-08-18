from datetime import timedelta

import pandas as pd

from engine.darts_pipeline.builder import build_model
from engine.darts_pipeline.runner import run_backtest, run_forecast
from engine.darts_pipeline.spec import BacktestSpec
from engine.ingestion.internal_db import populate_internal_db
from engine.ingestion.temp_utils import populate_externals_dbs
from engine.logging_config import logger, setup_logging
from engine.model_configs import model_hourly
from engine.scenario.future_scenario import random_future_scenario
from engine.series_utils import (
    get_covariate_ts,
    get_load_ts,
)

setup_logging(level="INFO")


if __name__ == "__main__":
    populate_externals_dbs()
    populate_internal_db()

    #  -- SELECT THE MODEL ---------
    model_config = model_hourly["tft_V1"]
    forecast_horizon = 24

    #  -- PRE PROCESSING ---------

    # Build Darts TimeSeries from the database
    series = get_load_ts()
    start_date, end_date = series.start_time(), series.end_time()
    assert isinstance(start_date, pd.Timestamp) and isinstance(end_date, pd.Timestamp)

    logger.info(
        f"Target series: {len(series)} steps, freq={series.freq}, "
        f"span={start_date} -> {end_date}"
    )

    future_cov = get_covariate_ts(start_date, end_date)
    logger.info(
        f"Future covariates: {len(future_cov)} steps, "
        f"{future_cov.n_components} component(s)"
    )

    #  -- BACKTEST ---------
    spec = BacktestSpec(
        forecast_horizon=forecast_horizon,
        stride=24 * 7,  # 1 week between origins
        retrain=True,
        start=pd.Timestamp(year=2020, month=1, day=1),  # ty: ignore[invalid-argument-type]
    )

    logger.info("Running backtest (this may take a moment)…")
    run_backtest(model_config, spec, series, future_cov=future_cov)

    #  -- FORECAST ---------

    forecast_start = end_date - timedelta(hours=forecast_horizon - 1)

    max_future_covariate_lag = build_model(model_config).extreme_lags[5] or 0
    forecast_horizon_needed = max(forecast_horizon, max_future_covariate_lag + 1)
    forecast_end = forecast_start + timedelta(hours=forecast_horizon_needed)

    forecast_training_series = series.drop_after(forecast_start)
    future_scenario = random_future_scenario(
        model_config["feature_subset"],
        forecast_start,
        forecast_end,
    )
    logger.info(f"Fitting and forecasting from {forecast_start} to {forecast_end}")
    fcst = run_forecast(
        model_config,
        forecast_training_series,
        forecast_horizon,
        future_cov=future_cov,
        future_scenario=future_scenario,
    )
    logger.info(fcst.to_dataframe())

    logger.success("Pipeline complete.")
