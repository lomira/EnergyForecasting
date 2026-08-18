"""Darts model configuration dictionaries.

Each module declares one or more configs with the shape:
    model_cls       — the Darts ForecastingModel class
    train_length    — training window used by backtests and forecasts
    hyperparams     — kwargs passed to the model constructor
    feature_subset  — columns to select from covariates
    target_transform_chain    — factory tuples for the target pipeline
    future_cov_transform_chain — factory tuples for future covariates

"""

from . import lightgbm, nbeats, tft  # noqa: F401
from .registry import model_hourly as model_hourly
