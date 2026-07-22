"""LightGBM baseline configuration."""

from darts.dataprocessing.transformers import Scaler
from darts.models import LightGBMModel
from sklearn.preprocessing import RobustScaler

from engine.featurize.lags import RollingLagTransformer

LIGHTGBM_CONFIG = {
    "name": "lightgbm_baseline",
    "model_cls": LightGBMModel,
    "hyperparams": {
        "lags": [-1, -2, -24, -48, -168],
        "output_chunk_length": 24,
        "num_leaves": 63,
        "add_encoders": {
            "cyclic": {"future": ["hour", "dayofweek"]},
            "datetime_attribute": {"future": ["month"]},
            "tz": "UTC",
        },
    },
    "feature_subset": ("temperature_2m",),
    "target_transform_chain": (),  # trees are scale-invariant no need to scale
    "past_cov_transform_chain": (
        RollingLagTransformer(windows=(24, 168), stats=("mean", "std"), lag=24),
    ),
    "future_cov_transform_chain": (Scaler(RobustScaler()),),
}
