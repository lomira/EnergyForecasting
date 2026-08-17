import pandas as pd
from darts import TimeSeries

from engine.darts_pipeline import BacktestSpec, build_model, run_backtest
from engine.ingestion.temp_utils import populate_dbs
from engine.logging_config import logger, setup_logging
from engine.model_configs import REGISTERED_MODELS
from engine.series_utils import covariates_time_series, get_load_ts

setup_logging(level="INFO")


if __name__ == "__main__":
    populate_dbs()

    # region Select the model
    model_config = REGISTERED_MODELS["lightgbm_nex"]

    # region Preprocessing
    # Build Darts TimeSeries from the database
    series = get_load_ts()
    start_date, end_date = series.start_time(), series.end_time()

    logger.info(
        f"Target series: {len(series)} steps, freq={series.freq}, "
        f"span={series.start_time()} -> {series.end_time()}"
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

    # region Backtest
    spec = BacktestSpec(
        forecast_horizon=24,
        stride=24 * 7,  # 1 week between origins
        retrain=True,
        start=pd.Timestamp("2020-01-01"),
    )

    logger.info("Running LightGBM backtest (this may take a moment)…")
    result = run_backtest(model_config, spec, series, future_cov=future_cov)

    # --- One-shot forecast (fit on the model's training window, predict 24h ahead) ---
    # The forecast needs future covariates for the 24h beyond the data end
    train_length = model_config["train_length"]
    training_series = series[-train_length:]
    training_cov = future_cov.slice_intersect(training_series)
    logger.info(
        f"Fitting LightGBM on the last {train_length} steps and forecasting 24h ahead…"
    )
    model = build_model(model_config)
    model.fit(training_series, future_covariates=training_cov)
    # Build future covariates beyond the training data end for the forecast horizon.
    # Models without explicit future-covariate lags only need the forecast horizon.
    fcst_start = series.end_time() + pd.Timedelta(hours=1)
    extra_hours = 24 + max(
        model_config["hyperparams"].get("lags_future_covariates", [0])
    )
    fcst_end = fcst_start + pd.Timedelta(hours=extra_hours - 1)
    fcst_dates = pd.date_range(fcst_start, fcst_end, freq="h")
    # Use the last available covariate values as a proxy for the forecast horizon
    last_cov = future_cov.to_dataframe().iloc[-1:]
    fcst_cov_df = pd.DataFrame(
        index=fcst_dates, columns=future_cov.to_dataframe().columns
    )
    for col in fcst_cov_df.columns:
        fcst_cov_df[col] = last_cov[col].values[0]
    history_hours = max(getattr(model, "input_chunk_length", 1), 1)
    cov_start = series.end_time() - pd.Timedelta(hours=history_hours - 1)
    history_cov_df = future_cov.slice(cov_start, series.end_time()).to_dataframe()
    fcst_cov = TimeSeries.from_dataframe(pd.concat((history_cov_df, fcst_cov_df)))
    fcst = model.predict(n=24, future_covariates=fcst_cov)
    logger.info(
        f"\n{'=' * 60}\n"
        f"  24-hour ahead forecast\n"
        f"{'=' * 60}\n"
        f"{fcst.to_dataframe().to_string()}\n"
        f"{'=' * 60}"
    )

    logger.info("Pipeline complete.")
