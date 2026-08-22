"""Temporal Fusion Transformer configuration."""

import os

import torch
from darts.dataprocessing.transformers import Scaler
from darts.models import TFTModel
from sklearn.preprocessing import RobustScaler
from torch import nn

from engine.features.names import Feature
from engine.model_configs.registry import register_hourly


@register_hourly
def TFT_DEFAULT():
    """TFT with Darts' documented defaults made explicit."""
    return {
        "model_cls": TFTModel,
        "train_length": 24 * 21,
        "hyperparams": {
            # TFT-specific parameters; chunk lengths are required by Darts.
            "input_chunk_length": 336,
            "output_chunk_length": 24,
            "output_chunk_shift": 0,
            "train_sample_shape": None,
            "hidden_size": 16,
            "lstm_layers": 1,
            "num_attention_heads": 4,
            "full_attention": False,
            "feed_forward": "GatedResidualNetwork",
            "dropout": 0.1,
            "hidden_continuous_size": 8,
            "categorical_embedding_sizes": None,
            "add_relative_index": False,
            "skip_interpolation": False,
            "loss_fn": None,
            "likelihood": None,
            "norm_type": "LayerNorm",
            "use_static_covariates": True,
            # TorchForecastingModel / PLForecastingModule defaults.
            "batch_size": 32,
            "n_epochs": 100,
            "model_name": None,
            "work_dir": os.path.join(os.getcwd(), "darts_logs"),
            "log_tensorboard": False,
            "nr_epochs_val_period": 1,
            "force_reset": False,
            "save_checkpoints": False,
            "add_encoders": None,
            "random_state": 0,
            "pl_trainer_kwargs": {
                "enable_progress_bar": False,
                "enable_model_summary": False,
            },
            "show_warnings": False,
            "enable_finetuning": None,
            "torch_metrics": None,
            "optimizer_cls": torch.optim.Adam,
            "optimizer_kwargs": None,
            "lr_scheduler_cls": None,
            "lr_scheduler_kwargs": None,
            "use_reversible_instance_norm": False,
        },
        "feature_subset": (),
        "target_transform_chain": (),
        "future_cov_transform_chain": (),
    }


@register_hourly
def tft_V1():
    config = TFT_DEFAULT()
    config["hyperparams"].update(
        hidden_size=32,
        batch_size=64,
        n_epochs=2,
        add_encoders={
            "cyclic": {"future": ["hour", "dayofweek"]},
            "datetime_attribute": {"future": ["month"]},
            "tz": "UTC",
        },
    )
    config["feature_subset"] = (
        Feature.ALGER_TEMPERATURE_2M,
        Feature.ALGER_TEMPERATURE_2M_ROLL_MEAN24_LAG24,
        Feature.ALGER_TEMPERATURE_2M_ROLL_STD24_LAG24,
        Feature.ALGER_TEMPERATURE_2M_ROLL_MEAN168_LAG24,
        Feature.ALGER_TEMPERATURE_2M_ROLL_STD168_LAG24,
    )
    config["target_transform_chain"] = (Scaler(RobustScaler()),)
    config["future_cov_transform_chain"] = (Scaler(RobustScaler()),)
    return config


@register_hourly
def tft_deterministic_V1():
    """Deterministic TFT variant trained with mean squared error."""
    config = tft_V1()
    config["hyperparams"]["loss_fn"] = nn.MSELoss()
    return config
