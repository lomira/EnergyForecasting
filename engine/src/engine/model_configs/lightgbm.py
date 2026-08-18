"""LightGBM baseline configuration."""

from darts.models import LightGBMModel

from engine.featurize.calendar import encode_onehot_custom_weekday
from engine.model_configs.registry import register_hourly


@register_hourly
def lightgbm_V1():
    return {
        "model_cls": LightGBMModel,
        "train_length": 24 * 21,
        "hyperparams": {
            "lags": [-1, -2, -24, -48, -168],
            "lags_future_covariates": [0, 1, 2, 23, 24, 25],
            "output_chunk_length": 24,
            "num_leaves": 63,
            "verbose": -1,
            "add_encoders": {
                "cyclic": {"future": ["hour"]},
                "custom": {"future": [encode_onehot_custom_weekday]},
                "tz": "UTC",
            },
        },
        "feature_subset": ("Alger_temperature_2m",),
    }


@register_hourly
def lightgbm_nex():
    return {
        "model_cls": LightGBMModel,
        "train_length": 24 * 21,
        "hyperparams": {
            "lags": [-1, -2, -24, -48, -168],
            "lags_future_covariates": [0, 1, 2, 23, 24, 25],
            "output_chunk_length": 24,
            "num_leaves": 63,
            "verbose": -1,
            "add_encoders": {
                "cyclic": {"future": ["hour"]},
                "custom": {"future": [encode_onehot_custom_weekday]},
                "tz": "UTC",
            },
        },
        "feature_subset": (),
    }
