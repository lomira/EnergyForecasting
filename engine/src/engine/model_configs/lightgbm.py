"""LightGBM baseline configuration."""

from darts.models import LightGBMModel

LIGHTGBM_CONFIG = {
    "name": "lightgbm_baseline",
    "model_cls": LightGBMModel,
    "hyperparams": {
        "lags": [-1, -2, -24, -48, -168],
        "lags_future_covariates": [0, 1, 2, 23, 24, 25],
        "output_chunk_length": 24,
        "num_leaves": 63,
        "verbose": -1,
    },
    "feature_subset": (),
    "target_transform_chain": (),
    "past_cov_transform_chain": (),
    "future_cov_transform_chain": (),
}
