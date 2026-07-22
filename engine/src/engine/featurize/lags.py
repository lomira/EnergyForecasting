"""Rolling-window lag features as a Darts-native transformer.

Covariate pipelines ONLY (not invertible).

NaN-head policy: the first `lag + max(windows) - 1` rows are undefined.
We backfill with the first valid value computed from data STRICTLY INSIDE the transformed slice, no leak.
"""

from typing import Sequence

import pandas as pd
from darts import TimeSeries
from darts.dataprocessing.transformers import DataTransformer


class RollingLagTransformer(DataTransformer):
    def __init__(
        self,
        windows: Sequence[int] = (24, 168),
        stats: Sequence[str] = ("mean", "std"),
        lag: int = 24,
        fill: str = "first_valid",
        name: str | None = None,
        n_jobs: int = 1,
        verbose: bool = False,
    ) -> None:
        self._windows = tuple(int(w) for w in windows)
        self._stats = tuple(str(s) for s in stats)
        self._lag = int(lag)
        self._fill = str(fill)
        super().__init__(
            name=name
            or f"RollingLag(lag={self._lag},w={self._windows},stats={self._stats})",
            n_jobs=n_jobs,
            verbose=verbose,
        )

    @staticmethod
    def ts_transform(series: TimeSeries, params: dict) -> TimeSeries:
        fixed = params["fixed"]
        windows, stats = fixed["_windows"], fixed["_stats"]
        lag, fill = fixed["_lag"], fixed["_fill"]

        df = series.pd_dataframe()
        feats: dict[str, pd.Series] = {}
        for col in df.columns:
            shifted = df[col].shift(lag)  # strictly-past values only
            for w in windows:
                roll = shifted.rolling(w, min_periods=w)  # NO centered windows, ever
                for s in stats:
                    feats[f"{col}__roll_{s}{w}_lag{lag}"] = getattr(roll, s)()

        out = pd.concat([df, pd.DataFrame(feats, index=df.index)], axis=1)
        if fill == "first_valid":
            out = out.bfill().ffill()  # bfill source is the first VALID value in-slice
        elif fill != "none":
            raise ValueError(f"unknown fill policy: {fill}")

        return TimeSeries.from_dataframe(out)
