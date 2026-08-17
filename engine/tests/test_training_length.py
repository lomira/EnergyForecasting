from types import SimpleNamespace
from unittest import TestCase

from engine.darts_pipeline.runner import _hf_kwargs
from engine.model_configs import REGISTERED_MODELS


class TrainingLengthTests(TestCase):
    def test_models_define_their_training_length(self) -> None:
        self.assertEqual(
            {config["train_length"] for config in REGISTERED_MODELS.values()},
            {24 * 21},
        )

    def test_backtest_uses_the_model_training_length(self) -> None:
        spec = SimpleNamespace(
            forecast_horizon=24,
            stride=168,
            start=None,
            retrain=True,
            overlap_end=False,
            last_points_only=True,
        )

        self.assertEqual(_hf_kwargs({"train_length": 123}, spec)["train_length"], 123)
