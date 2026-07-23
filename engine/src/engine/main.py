import os
from datetime import datetime

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "engine.django_settings")
django.setup()

from django.conf import settings  # noqa: E402
from django.core.management import call_command  # noqa: E402

from engine.ingestion.get_all_covariates import get_all_covariates  # noqa: E402
from engine.ingestion.get_holidays import get_holidays  # noqa: E402
from engine.ingestion.load_ingestion import (  # noqa: E402
    add_load_excel_to_db,
    get_load_start_end_dates,
)
from engine.ingestion.weather_ingestion import get_weather_data  # noqa: E402
from engine.logging_config import logger, setup_logging  # noqa: E402

setup_logging()

if __name__ == "__main__":
    # --- Ingestion phase ---
    # Remove the database to ensure a clean rebuild each time
    # The weather cache is kept separate
    db_path = settings.ENGINE_DB_ROOT / settings.ENGINE_SQLITE_FILENAME
    db_path.unlink(missing_ok=True)

    # Ensure the DB directory exists
    # Then create the schema directly from the model definitions
    # Migrations are disabled in settings
    settings.ENGINE_DB_ROOT.mkdir(parents=True, exist_ok=True)
    call_command("migrate", run_syncdb=True, verbosity=1)
    logger.info(f"Created SQLite database at {db_path}")

    add_load_excel_to_db(
        file_path=settings.ENGINE_RAW_EXCEL_ROOT / "BDD_E.xlsx",
        sheet_name="Feuil1",
        db_path=db_path,
    )

    start_date, end_date = get_load_start_end_dates(db_path)
    start_date = datetime(2016, 1, 1)  # Overrides the start date
    get_holidays(start_date, end_date)
    get_weather_data(start_date, end_date)

    cov = get_all_covariates(start_date, end_date)
    logger.info(f"Built covariate frame with {len(cov):,.0f} rows")

    # --- Forecasting phase ---
    import pandas as pd
    from darts import TimeSeries

    from engine.darts_pipeline import BacktestSpec, build_model, run_backtest
    from engine.model_configs.lightgbm import LIGHTGBM_CONFIG
    from engine.series_utils import covariates_time_series, load_time_series

    # Build Darts TimeSeries from the database
    series = load_time_series(start_date, end_date)
    logger.info(
        f"Target series: {len(series)} steps, freq={series.freq}, "
        f"span={series.start_time()} -> {series.end_time()}"
    )

    future_cov = covariates_time_series(
        start_date,
        end_date,
        feature_subset=LIGHTGBM_CONFIG["feature_subset"],
    )
    logger.info(
        f"Future covariates: {len(future_cov)} steps, "
        f"{future_cov.n_components} component(s)"
    )

    # --- Backtest ---
    # Day-ahead hourly forecast, one per day, training on the last 8 weeks
    spec = BacktestSpec(
        forecast_horizon=24,
        stride=24 * 7,  # 1 week between origins
        train_length=24 * 14,  # 2 weeks of hourly data
        retrain=True,
        start=pd.Timestamp("2020-01-01"),
    )

    logger.info("Running LightGBM backtest (this may take a moment)…")
    result = run_backtest(LIGHTGBM_CONFIG, spec, series, future_cov=future_cov)
    logger.info(
        f"Backtest complete: {len(result.forecasts)} origins, "
        f"aggregate WAPE = {result.aggregate:.4f}"
    )
    print(f"\n{'=' * 60}")
    print("  LightGBM backtest results")
    print(f"{'=' * 60}")
    print(f"  Origins:       {len(result.forecasts)}")
    print(f"  Aggregate WAPE: {result.aggregate:.4f} ({result.aggregate * 100:.2f}%)")
    print(f"  Spec hash:     {result.spec_hash}")
    print(f"  Config hash:   {result.config_hash}")
    print(f"  Data fp:       {result.data_fp}")
    if result.fold_scores:
        print(
            f"  Fold scores:   min={min(result.fold_scores):.4f}, "
            f"max={max(result.fold_scores):.4f}, "
            f"median={sorted(result.fold_scores)[len(result.fold_scores) // 2]:.4f}"
        )
    print(f"{'=' * 60}\n")

    # --- One-shot forecast (fit on full data, predict 24h ahead) ---
    # The forecast needs future covariates for the 24h beyond the data end
    logger.info("Fitting LightGBM on full data and forecasting 24h ahead…")
    model = build_model(LIGHTGBM_CONFIG)
    model.fit(series, future_covariates=future_cov)
    # Build future covariates beyond the training data end for the forecast horizon
    # lags_future_covariates=[0,1,2,23,24,25] + output_chunk=24 => need 49 extra hours
    fcst_start = series.end_time() + pd.Timedelta(hours=1)
    extra_hours = 24 + max(LIGHTGBM_CONFIG["hyperparams"]["lags_future_covariates"])
    fcst_end = fcst_start + pd.Timedelta(hours=extra_hours - 1)
    fcst_dates = pd.date_range(fcst_start, fcst_end, freq="h")
    # Use the last available covariate values as a proxy for the forecast horizon
    last_cov = future_cov.to_dataframe().iloc[-1:]
    fcst_cov_df = pd.DataFrame(
        index=fcst_dates, columns=future_cov.to_dataframe().columns
    )
    for col in fcst_cov_df.columns:
        fcst_cov_df[col] = last_cov[col].values[0]
    fcst_cov = TimeSeries.from_dataframe(fcst_cov_df)
    fcst = model.predict(n=24, future_covariates=fcst_cov)
    logger.info(
        f"Forecast: {len(fcst)} steps, span={fcst.start_time()} -> {fcst.end_time()}"
    )
    print(f"\n{'=' * 60}")
    print("  24-hour ahead forecast")
    print(f"{'=' * 60}")
    print(fcst.to_dataframe().to_string())
    print(f"{'=' * 60}\n")

    logger.info("Pipeline complete.")
