"""ModelConfig -> Darts model + transform pipelines.

Pipelines are built FRESH (unfitted) from factories every time
No fitted state ever lives in a config dict.
"""

from typing import Any

from darts.dataprocessing.pipeline import Pipeline
from darts.models.forecasting.forecasting_model import ForecastingModel


def build_model(config: dict, **extra: Any) -> ForecastingModel:
    """Unfitted Darts model from a config dict.

    Parameters
    ----------
    config : dict
        Must contain ``model_cls`` and ``hyperparams`` keys.
    extra : Any
        Additional kwargs merged into hyperparams (for Optuna trials).
    """
    model_cls: type[ForecastingModel] = config["model_cls"]
    hyperparams = {**dict(config.get("hyperparams", {})), **extra}
    return model_cls(**hyperparams)


def build_data_transformers(config: dict) -> dict[str, Pipeline]:
    """Build the ``data_transformers`` dict expected by Darts ``historical_forecasts``.

    Returns a dict with optional keys:
        - ``"series"``: target pipeline
        - ``"past_covariates"``: past-covariate pipeline
        - ``"future_covariates"``: future-covariate pipeline
    """
    dt: dict[str, Pipeline] = {}

    target_chain = config.get("target_transform_chain", ())
    if target_chain:
        dt["series"] = Pipeline(list(target_chain))

    past_chain = config.get("past_cov_transform_chain", ())
    if past_chain:
        dt["past_covariates"] = Pipeline(list(past_chain))

    fut_chain = config.get("future_cov_transform_chain", ())
    if fut_chain:
        dt["future_covariates"] = Pipeline(list(fut_chain))

    return dt
