import pandas as pd

from engine.darts_pipeline import BacktestSpec, build_model, run_backtest, run_forecast
from engine.ingestion.temp_utils import populate_dbs
from engine.logging_config import logger, setup_logging
from engine.model_configs import REGISTERED_MODELS
from engine.scenario.future_scenario import random_future_scenario
from engine.series_utils import (
    covariates_time_series,
    get_load_ts,
)

setup_logging(level="INFO")


if __name__ == "__main__":
    populate_dbs()

    #  -- SELECT THE MODEL ---------
    model_config = REGISTERED_MODELS["tft_V1"]
    forecast_horizon = 24

    #  -- PRE PROCESSING ---------

    # Build Darts TimeSeries from the database
    series = get_load_ts()
    start_date, end_date = series.start_time(), series.end_time()

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
    max_future_covariate_lag = build_model(model_config).extreme_lags[5] or 0
    forecast_end = (
        forecast_start
        + max(forecast_horizon - 1, max_future_covariate_lag) * series.freq
    )

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
