"""N-BEATS configuration."""

from darts.dataprocessing.transformers import Scaler
from darts.models import NBEATSModel
from sklearn.preprocessing import RobustScaler

NBEATS_CONFIG = {
    "name": "nbeats",
    "model_cls": NBEATSModel,
    "hyperparams": {
        "input_chunk_length": 336,
        "output_chunk_length": 24,
        "num_blocks": 3,
        "num_layers": 4,
        "layer_widths": 256,
        "batch_size": 64,
        "n_epochs": 50,
    },
    "feature_subset": (),
    "target_transform_chain": (Scaler(RobustScaler()),),
    "past_cov_transform_chain": (),
    "future_cov_transform_chain": (),
}
