"""LightGBM baseline configuration."""

from darts.models import LightGBMModel

from engine.featurize.calendar import encode_onehot_custom_weekday
from engine.featurize.features import Feature
from engine.model_configs.registry import register_hourly
from engine.peak_reconciled_lightgbm import PeakReconciledLightGBMModel


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
        "feature_subset": (Feature.ALGER_TEMPERATURE_2M,),
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


@register_hourly
def peak_reconciled_lightgbm_V1():
    return {
        "model_cls": PeakReconciledLightGBMModel,
        # Max sure it is at least as long as the daily model's train length * 24
        "train_length": 24 * 365 * 2,
        "hyperparams": {
            "hourly_train_length": 24 * 21,
            "daily_train_length": 365 * 2,
            "hourly_hyperparams": {
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
            "daily_hyperparams": {
                "lags": [-1, -2, -7, -14, -28, -364, -365],
                "lags_future_covariates": [0],
                "output_chunk_length": 1,
                "num_leaves": 31,
                "verbose": -1,
                "add_encoders": {
                    "cyclic": {"future": ["dayofweek"]},
                    "datetime_attribute": {"future": ["month"]},
                    "tz": "UTC",
                },
            },
        },
        "feature_subset": (Feature.ALGER_TEMPERATURE_2M,),
    }
