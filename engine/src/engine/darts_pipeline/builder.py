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


def _build_pipeline(chain: tuple[Any, ...] | tuple[()]) -> Pipeline | None:
    """Build a pipeline from a tuple of factory results (or None if empty)."""
    if not chain:
        return None
    return Pipeline(list(chain)) if len(chain) > 1 else chain[0]


def build_target_pipeline(config: dict) -> Pipeline | None:
    """Build the target transform pipeline."""
    chain = config.get("target_transform_chain", ())
    return _build_pipeline(chain)


def build_data_transformers(config: dict) -> dict[str, Pipeline]:
    """Build the ``data_transformers`` dict expected by Darts ``historical_forecasts``.

    Returns a dict with optional keys:
        - ``"series"``: target pipeline
        - ``"past_covariates"``: past-covariate pipeline
        - ``"future_covariates"``: future-covariate pipeline
    """
    dt: dict[str, Pipeline] = {}
    target = build_target_pipeline(config)
    if target is not None:
        dt["series"] = target

    past = _build_pipeline(config.get("past_cov_transform_chain", ()))
    if past is not None:
        dt["past_covariates"] = past

    fut = _build_pipeline(config.get("future_cov_transform_chain", ()))
    if fut is not None:
        dt["future_covariates"] = fut

    return dt
