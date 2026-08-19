"""Hourly LightGBM forecast reconciled to a second model's daily peaks."""

from typing import cast

import numpy as np
import pandas as pd
from darts import TimeSeries
from darts.models import LightGBMModel
from darts.models.forecasting.forecasting_model import GlobalForecastingModel
from darts.typing import TimeSeriesLike

HOURS_PER_DAY = 24


def aggregate_blocks(
    series: TimeSeries,
    start: pd.Timestamp,
    blocks: int,
    method: str,
) -> TimeSeries:
    """Aggregate consecutive prediction-aligned 24-hour blocks."""
    frequency = cast(pd.DateOffset, series.freq)
    end = start + (blocks * HOURS_PER_DAY - 1) * frequency
    values = series.slice(start, end)
    expected = blocks * HOURS_PER_DAY
    if len(values) != expected:
        raise ValueError(
            f"Series does not cover {blocks} complete 24-hour blocks from {start}"
        )
    frame = values.to_dataframe()
    grouped = frame.groupby(np.arange(expected) // HOURS_PER_DAY)
    daily = grouped.max() if method == "max" else grouped.mean()
    daily.index = pd.date_range(
        start, periods=blocks, freq="24h", name=frame.index.name
    )
    return TimeSeries.from_dataframe(daily)


def reconcile_daily_peaks(
    hourly_forecast: TimeSeries, daily_peaks: TimeSeries
) -> TimeSeries:
    """Scale each hourly block so its maximum equals the daily prediction."""
    if len(hourly_forecast) != len(daily_peaks) * HOURS_PER_DAY:
        raise ValueError("Hourly forecast and daily peaks cover different horizons")
    if hourly_forecast.n_components != 1 or daily_peaks.n_components != 1:
        raise ValueError("Peak reconciliation requires univariate forecasts")

    values = hourly_forecast.values(copy=True)
    for day, peak in enumerate(daily_peaks.univariate_values(copy=False)):
        block = values[day * HOURS_PER_DAY : (day + 1) * HOURS_PER_DAY]
        hourly_peak = float(block.max())
        if hourly_peak <= 0 or peak <= 0:
            raise ValueError("Peak reconciliation requires positive forecasts")
        block *= float(peak) / hourly_peak
    return hourly_forecast.with_values(values)


class PeakReconciledLightGBMModel(GlobalForecastingModel):
    """Compose hourly and daily LightGBMs behind the normal Darts interface."""

    def __init__(
        self,
        hourly_train_length: int,
        daily_train_length: int,
        hourly_hyperparams: dict,
        daily_hyperparams: dict,
    ):
        if hourly_train_length < 1 or daily_train_length < 1:
            raise ValueError("Training lengths must be positive")
        self.hourly_train_length = hourly_train_length
        self.daily_train_length = daily_train_length
        self.hourly_hyperparams = hourly_hyperparams
        self.daily_hyperparams = daily_hyperparams
        super().__init__(add_encoders=None)
        self._hourly_model = LightGBMModel(**hourly_hyperparams)
        self._daily_model = LightGBMModel(**daily_hyperparams)

    @property
    def required_train_length(self) -> int:
        return max(
            self.hourly_train_length,
            self.daily_train_length * HOURS_PER_DAY,
        )

    @staticmethod
    def _validate_series(series: TimeSeries) -> None:
        if series.freq != pd.tseries.frequencies.to_offset("h"):
            raise ValueError("PeakReconciledLightGBMModel requires hourly data")
        if series.n_components != 1 or not series.is_deterministic:
            raise ValueError(
                "PeakReconciledLightGBMModel requires a deterministic univariate target"
            )

    def _daily_target(self, series: TimeSeries) -> TimeSeries:
        frequency = cast(pd.DateOffset, series.freq)
        start = (
            cast(pd.Timestamp, series.end_time())
            + frequency
            - self.daily_train_length * HOURS_PER_DAY * frequency
        )
        return aggregate_blocks(series, start, self.daily_train_length, "max")

    def _daily_covariates(
        self, covariates: TimeSeries, target: TimeSeries, future_days: int = 0
    ) -> TimeSeries:
        return aggregate_blocks(
            covariates,
            cast(pd.Timestamp, target.start_time()),
            len(target) + future_days,
            "mean",
        )

    def fit(
        self,
        series: TimeSeriesLike,
        past_covariates: TimeSeriesLike | None = None,
        future_covariates: TimeSeriesLike | None = None,
        verbose: bool | None = None,
    ) -> PeakReconciledLightGBMModel:
        if not isinstance(series, TimeSeries):
            raise TypeError("PeakReconciledLightGBMModel supports one series at a time")
        if past_covariates is not None:
            raise ValueError("Past covariates are not supported")
        if not isinstance(future_covariates, TimeSeries):
            raise TypeError("Hourly future covariates are required")
        self._validate_series(series)
        if len(series) < self.required_train_length:
            raise ValueError(
                f"series has {len(series)} steps; model requires "
                f"{self.required_train_length}"
            )
        if future_covariates.freq != series.freq:
            raise ValueError("future_covariates freq mismatch")

        super().fit(series, future_covariates=future_covariates, verbose=verbose)
        daily_target = self._daily_target(series)
        self._hourly_model.fit(
            series[-self.hourly_train_length :],
            future_covariates=future_covariates,
            verbose=verbose,
        )
        self._daily_model.fit(
            daily_target,
            future_covariates=self._daily_covariates(future_covariates, daily_target),
            verbose=verbose,
        )
        return self

    def predict(
        self,
        n: int,
        series: TimeSeriesLike | None = None,
        past_covariates: TimeSeriesLike | None = None,
        future_covariates: TimeSeriesLike | None = None,
        num_samples: int = 1,
        verbose: bool | None = None,
        predict_likelihood_parameters: bool = False,
        show_warnings: bool = True,
        random_state: int | None = None,
    ) -> TimeSeries:
        if n < HOURS_PER_DAY or n % HOURS_PER_DAY:
            raise ValueError("horizon must be a positive multiple of 24 hours")
        if past_covariates is not None:
            raise ValueError("Past covariates are not supported")
        if predict_likelihood_parameters:
            raise ValueError("Likelihood parameter prediction is not supported")
        input_series = series if series is not None else self.training_series
        covariates = (
            future_covariates
            if future_covariates is not None
            else self.future_covariate_series
        )
        if not isinstance(input_series, TimeSeries) or not isinstance(
            covariates, TimeSeries
        ):
            raise TypeError("A target series and hourly future covariates are required")
        self._validate_series(input_series)

        super().predict(
            n=n,
            series=input_series,
            future_covariates=covariates,
            num_samples=num_samples,
            verbose=verbose,
            predict_likelihood_parameters=False,
            show_warnings=show_warnings,
            random_state=random_state,
        )
        hourly = cast(
            TimeSeries,
            self._hourly_model.predict(
                n=n,
                series=input_series[-self.hourly_train_length :],
                future_covariates=covariates,
                num_samples=num_samples,
                verbose=verbose,
                show_warnings=show_warnings,
                random_state=random_state,
            ),
        )
        daily_target = self._daily_target(input_series)
        future_days = n // HOURS_PER_DAY
        daily = cast(
            TimeSeries,
            self._daily_model.predict(
                n=future_days,
                series=daily_target,
                future_covariates=self._daily_covariates(
                    covariates, daily_target, future_days
                ),
                num_samples=num_samples,
                verbose=verbose,
                show_warnings=show_warnings,
                random_state=random_state,
            ),
        )
        return reconcile_daily_peaks(hourly, daily)

    @property
    def supports_multivariate(self) -> bool:
        return False

    @property
    def supports_future_covariates(self) -> bool:
        return True

    @property
    def supports_sample_weight(self) -> bool:
        return False

    @property
    def supports_optimized_historical_forecasts(self) -> bool:
        return False

    @property
    def _supports_non_retrainable_historical_forecasts(self) -> bool:
        return False

    @property
    def output_chunk_length(self) -> int:
        return HOURS_PER_DAY

    @property
    def min_train_samples(self) -> int:
        return 1

    @property
    def _target_window_lengths(self) -> tuple[int, int]:
        return self.required_train_length, 0

    @property
    def extreme_lags(
        self,
    ) -> tuple[int | None, int, int | None, int | None, int | None, int | None, int]:
        hourly = self._hourly_model.extreme_lags
        return (
            -self.required_train_length,
            HOURS_PER_DAY - 1,
            None,
            None,
            hourly[4],
            max(hourly[5] or 0, HOURS_PER_DAY - 1),
            0,
        )

    @property
    def _model_encoder_settings(
        self,
    ) -> tuple[None, None, bool, bool, None, None]:
        return None, None, False, False, None, None
