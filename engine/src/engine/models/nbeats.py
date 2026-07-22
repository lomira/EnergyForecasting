"""N-BEATS configuration."""

from darts.models import NBEATSModel

from engine.featurize.factories import robust_scaler

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
    "target_transform_chain": (robust_scaler(),),
    "past_cov_transform_chain": (),
    "future_cov_transform_chain": (),
}
