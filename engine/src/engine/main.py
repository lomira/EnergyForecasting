import pandas as pd

from engine.darts_pipeline import BacktestSpec, build_model, run_backtest, run_forecast
from engine.ingestion.temp_utils import populate_dbs
from engine.logging_config import logger, setup_logging
from engine.model_configs import model_hourly
from engine.scenario.future_scenario import random_future_scenario
from engine.series_utils import (
    covariates_time_series,
    get_load_ts,
)

setup_logging(level="INFO")


if __name__ == "__main__":
    populate_dbs()

    #  -- SELECT THE MODEL ---------
    model_config = model_hourly["tft_V1"]
    forecast_horizon = 24

    #  -- PRE PROCESSING ---------

    # Build Darts TimeSeries from the database
    series = get_load_ts()
    start_date, end_date = series.start_time(), series.end_time()
    if type(start_date) is not pd.Timestamp or type(end_date) is not pd.Timestamp:
        raise ValueError("series start and end must be pd.Timestamp")

    logger.info(
        f"Target series: {len(series)} steps, freq={series.freq}, "
        f"span={start_date} -> {end_date}"
    )

    future_cov = covariates_time_series(
        start_date,
        end_date,
        feature_subset=model_config["feature_subset"],
    )
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
    result = run_backtest(model_config, spec, series, future_cov=future_cov)

    #  -- FORECAST ---------

    forecast_start = end_date - pd.Timedelta(hours=forecast_horizon - 1)
    if type(forecast_start) is not pd.Timestamp:
        raise ValueError("forecast_start must be pd.Timestamp")

    max_future_covariate_lag = build_model(model_config).extreme_lags[5] or 0
    forecast_horizon_needed = max(forecast_horizon, max_future_covariate_lag + 1)
    forecast_end = forecast_start + pd.Timedelta(hours=forecast_horizon_needed)
    if type(forecast_end) is not pd.Timestamp:
        raise ValueError("forecast_end must be pd.Timestamp")

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
