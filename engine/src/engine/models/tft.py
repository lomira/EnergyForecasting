"""Temporal Fusion Transformer configuration."""

from darts.models import TFTModel

from engine.featurize.factories import robust_scaler, rolling_lags

TFT_CONFIG = {
    "name": "tft",
    "model_cls": TFTModel,
    "hyperparams": {
        "input_chunk_length": 336,
        "output_chunk_length": 24,
        "hidden_size": 32,
        "lstm_layers": 1,
        "num_attention_heads": 4,
        "dropout": 0.1,
        "batch_size": 64,
        "n_epochs": 20,
        "add_relative_index": True,
        "add_encoders": {
            "cyclic": {"future": ["hour", "dayofweek"]},
            "datetime_attribute": {"future": ["month"]},
            "tz": "UTC",
        },
    },
    "feature_subset": ("temperature_2m",),
    "target_transform_chain": (robust_scaler(),),
    "past_cov_transform_chain": (
        rolling_lags(windows=(24, 168), stats=("mean", "std"), lag=24),
        robust_scaler(),
    ),
    "future_cov_transform_chain": (robust_scaler(),),
}
