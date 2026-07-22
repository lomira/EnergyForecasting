"""Darts model configuration dictionaries.

Each module exports a single CONFIG dict with the shape:
    model_cls       — the Darts ForecastingModel class
    hyperparams     — kwargs passed to the model constructor
    feature_subset  — columns to select from covariates
    target_transform_chain    — factory tuples for the target pipeline
    past_cov_transform_chain  — factory tuples for past covariates
    future_cov_transform_chain — factory tuples for future covariates

"""

from engine.model_configs.lightgbm import LIGHTGBM_CONFIG
from engine.model_configs.nbeats import NBEATS_CONFIG
from engine.model_configs.tft import TFT_CONFIG

REGISTERED_MODELS: dict[str, dict] = {
    "lightgbm_V1": LIGHTGBM_CONFIG,
    "tft_V1": TFT_CONFIG,
    "nbeats_V1": NBEATS_CONFIG,
}
