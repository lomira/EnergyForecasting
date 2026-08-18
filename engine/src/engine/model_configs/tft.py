"""Temporal Fusion Transformer configuration."""

from darts.dataprocessing.transformers import Scaler
from darts.models import TFTModel
from sklearn.preprocessing import RobustScaler

from engine.featurize.lags import RollingLagTransformer
from engine.model_configs.registry import register_hourly


@register_hourly
def tft_V1():
    return {
        "name": "tft",
        "model_cls": TFTModel,
        "train_length": 24 * 21,
        "hyperparams": {
            "input_chunk_length": 336,
            "output_chunk_length": 24,
            "hidden_size": 32,
            "lstm_layers": 1,
            "num_attention_heads": 4,
            "dropout": 0.1,
            "batch_size": 64,
            "n_epochs": 2,
            "add_relative_index": True,
            "add_encoders": {
                "cyclic": {"future": ["hour", "dayofweek"]},
                "datetime_attribute": {"future": ["month"]},
                "tz": "UTC",
            },
        },
        "feature_subset": ("Alger_temperature_2m",),
        "target_transform_chain": (Scaler(RobustScaler()),),
        "past_cov_transform_chain": (
            RollingLagTransformer(windows=(24, 168), stats=("mean", "std"), lag=24),
            Scaler(RobustScaler()),
        ),
        "future_cov_transform_chain": (Scaler(RobustScaler()),),
    }
