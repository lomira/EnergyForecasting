"""LightGBM baseline configuration."""

from darts.models import LightGBMModel

from engine.featurize.factories import robust_scaler, rolling_lags

LIGHTGBM_CONFIG = {
    "name": "lightgbm_baseline",
    "model_cls": LightGBMModel,
    "hyperparams": {
        "lags": [-1, -2, -24, -48, -168],
        "output_chunk_length": 24,
        "num_leaves": 63,
    },
    "feature_subset": (
        "temperature_2m",
        "hour_sin",
        "hour_cos",
        "dow_sin",
        "dow_cos",
        "is_holiday",
    ),
    "target_transform_chain": (),  # trees are scale-invariant no need to scale
    "past_cov_transform_chain": (
        rolling_lags(windows=(24, 168), stats=("mean", "std"), lag=24),
    ),
    "future_cov_transform_chain": (robust_scaler(),),
}
