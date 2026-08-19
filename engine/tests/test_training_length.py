import copy
from unittest import TestCase
from unittest.mock import patch

import numpy as np
import pandas as pd
import torch
from darts import TimeSeries
from darts.models import TFTModel
from torch import nn

from engine.darts_pipeline.builder import build_model
from engine.darts_pipeline.runner import run_backtest
from engine.darts_pipeline.spec import BacktestSpec
from engine.model_configs import model_hourly


class TrainingLengthTests(TestCase):
    def test_models_define_their_training_length(self) -> None:
        self.assertEqual(
            {
                config["train_length"]
                for name, config in model_hourly.items()
                if name != "peak_reconciled_lightgbm_V1"
            },
            {24 * 21},
        )
        self.assertEqual(
            model_hourly["peak_reconciled_lightgbm_V1"]["train_length"],
            24 * 365 * 2,
        )

    def test_models_register_by_function_name(self) -> None:
        self.assertEqual(
            set(model_hourly),
            {
                "lightgbm_V1",
                "lightgbm_nex",
                "peak_reconciled_lightgbm_V1",
                "tft_V1",
                "tft_deterministic_V1",
                "TFT_DEFAULT",
                "nbeats_V1",
            },
        )

    def test_deterministic_tft_uses_mse_loss(self) -> None:
        self.assertIsInstance(
            model_hourly["tft_deterministic_V1"]["hyperparams"]["loss_fn"],
            nn.MSELoss,
        )

    def test_tft_default_matches_darts_defaults_except_trainer_display(self) -> None:
        configured = build_model(model_hourly["TFT_DEFAULT"])
        direct = TFTModel(input_chunk_length=336, output_chunk_length=24)

        self.assertEqual(
            {key: configured.model_params[key] for key in direct.model_params},
            dict(direct.model_params),
        )
        self.assertEqual(
            {key: configured.pl_module_params[key] for key in direct.pl_module_params},
            dict(direct.pl_module_params),
        )
        self.assertEqual(
            {key: configured.trainer_params[key] for key in direct.trainer_params},
            direct.trainer_params,
        )
        self.assertEqual(
            {
                "enable_progress_bar": False,
                "enable_model_summary": False,
            },
            {
                key: configured.trainer_params[key]
                for key in ("enable_progress_bar", "enable_model_summary")
            },
        )

    def test_tft_default_forecast_matches_direct_model(self) -> None:
        config = copy.deepcopy(model_hourly["TFT_DEFAULT"])
        config["hyperparams"].update(
            n_epochs=1,
            pl_trainer_kwargs={
                "enable_progress_bar": False,
                "enable_model_summary": False,
                "logger": False,
            },
        )

        index = pd.date_range("2024-01-01", periods=384, freq="h")
        future_index = pd.date_range(index[0], periods=408, freq="h")
        target = TimeSeries.from_times_and_values(
            index, np.sin(np.arange(384) / 12).reshape(-1, 1)
        )
        future_covariates = TimeSeries.from_times_and_values(
            future_index, np.cos(np.arange(408) / 24).reshape(-1, 1)
        )

        torch.manual_seed(0)
        configured = build_model(config)
        configured.fit(target, future_covariates=future_covariates)
        torch.manual_seed(0)
        direct = TFTModel(
            input_chunk_length=336,
            output_chunk_length=24,
            n_epochs=1,
            random_state=config["hyperparams"]["random_state"],
            pl_trainer_kwargs=config["hyperparams"]["pl_trainer_kwargs"],
        )
        direct.fit(target, future_covariates=future_covariates)

        configured_forecast = configured.predict(
            24, future_covariates=future_covariates
        )
        direct_forecast = direct.predict(24, future_covariates=future_covariates)

        self.assertTrue(
            configured_forecast.time_index.equals(direct_forecast.time_index)
        )
        np.testing.assert_allclose(
            configured_forecast.all_values(), direct_forecast.all_values()
        )

    def test_backtest_uses_the_model_training_length(self) -> None:
        series = TimeSeries.from_times_and_values(
            pd.date_range("2024-01-01", periods=3, freq="h"), [1, 2, 3]
        )
        forecast = series[-1:]
        spec = BacktestSpec(
            forecast_horizon=24,
            stride=168,
            retrain=True,
            start=pd.Timestamp("2024-01-01"),
        )

        with (
            patch("engine.darts_pipeline.runner.build_model") as build_model,
            patch(
                "engine.darts_pipeline.runner.build_data_transformers", return_value={}
            ),
            patch("engine.darts_pipeline.runner.logger.info") as log_info,
        ):
            model = build_model.return_value
            model.supports_future_covariates = False
            model.historical_forecasts.return_value = [forecast]
            result = run_backtest({"train_length": 123}, spec, series)

        self.assertEqual(result, [forecast])
        self.assertEqual(
            model.historical_forecasts.call_args.kwargs["train_length"], 123
        )
        self.assertFalse(
            model.historical_forecasts.call_args.kwargs["last_points_only"]
        )
        self.assertTrue(
            any(
                "WAPE: 0.0000 (0.00%), MAPE: 0.0000 (0.00%)" in call.args[0]
                for call in log_info.call_args_list
            )
        )
