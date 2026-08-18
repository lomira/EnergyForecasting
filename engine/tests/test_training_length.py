from types import SimpleNamespace
from unittest import TestCase

from engine.darts_pipeline.runner import _hf_kwargs
from engine.model_configs import model_hourly


class TrainingLengthTests(TestCase):
    def test_models_define_their_training_length(self) -> None:
        self.assertEqual(
            {config["train_length"] for config in model_hourly.values()},
            {24 * 21},
        )

    def test_models_register_by_function_name(self) -> None:
        self.assertEqual(
            set(model_hourly),
            {"lightgbm_V1", "lightgbm_nex", "tft_V1", "nbeats_V1"},
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
