"""Darts model configuration dictionaries.

Each module exports a single CONFIG dict with the shape:
    model_cls       — the Darts ForecastingModel class
    train_length    — training window used by backtests and forecasts
    hyperparams     — kwargs passed to the model constructor
    feature_subset  — columns to select from covariates
    target_transform_chain    — factory tuples for the target pipeline
    past_cov_transform_chain  — factory tuples for past covariates
    future_cov_transform_chain — factory tuples for future covariates

"""

import engine.model_configs.lightgbm as lgbm
import engine.model_configs.nbeats as nbeast
import engine.model_configs.tft as tft

REGISTERED_MODELS: dict[str, dict] = {
    "lightgbm_V1": lgbm.LIGHTGBM_CONFIG_BASE,
    "lightgbm_nex": lgbm.LIGHTGBM_CONFIG_NOEXOGENEOUS,
    "tft_V1": tft.TFT_CONFIG,
    "nbeats_V1": nbeast.NBEATS_CONFIG,
}
