from datetime import timedelta

import pandas as pd

from engine.datasets.pipeline import initialize_source_databases, populate_internal_db
from engine.datasets.series import get_covariate_ts, get_load_ts
from engine.forecasting.builder import build_model
from engine.forecasting.runner import backtest_metrics, run_backtest, run_forecast
from engine.forecasting.spec import BacktestSpec
from engine.logging_config import logger, setup_logging
from engine.model_configs import model_hourly
from engine.scenarios import random_future_scenario
from engine.storage.backtests import save_backtest_result
from engine.storage.datasets import correct_loads_at

setup_logging(level="INFO")


if __name__ == "__main__":
    initialize_source_databases()
    populate_internal_db()

    # Loads correction
    correction = pd.DataFrame(
        {
            "datetime": ["2008-01-01 04:00:00", "2017-10-12 23:00:00"],
            "load_mw": [3160, 5656],
        }
    )
    correct_loads_at(correction)

    #  -- SELECT THE MODEL ---------
    model_config_name = "lightgbm_nex"
    model_config = model_hourly[model_config_name]
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
        stride=forecast_horizon,
        retrain=True,
        start=pd.Timestamp(year=2020, month=1, day=1),  # ty: ignore[invalid-argument-type]
    )

    logger.info("Running backtest (this may take a moment)…")
    backtest = run_backtest(model_config, spec, series, future_cov=future_cov)
    backtest_id = save_backtest_result(
        model_config_name,
        model_config,
        spec,
        series,
        backtest,
        metrics=backtest_metrics(series, backtest),
    )
    logger.success(f"Saved backtest result {backtest_id}")

    #  -- FORECAST ---------

    forecast_start = end_date - timedelta(hours=forecast_horizon - 1)

    max_future_covariate_lag = build_model(model_config).extreme_lags[5] or 0
    forecast_horizon_needed = max(forecast_horizon, max_future_covariate_lag + 1)
    forecast_end = forecast_start + timedelta(hours=forecast_horizon_needed)

    forecast_training_series = series.drop_after(forecast_start)
    # TODO: Replace this historical scenario with a live ECMWF IFS forecast.
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
    logger.info(f"\n {fcst.to_dataframe()}")

    logger.success("Pipeline complete.")
