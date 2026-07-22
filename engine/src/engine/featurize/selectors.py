"""Feature subsetting INSIDE the pipeline."""

from typing import Sequence

from darts import TimeSeries
from darts.dataprocessing.transformers import BaseDataTransformer


class ColumnSubsetTransformer(BaseDataTransformer):
    def __init__(
        self,
        columns: Sequence[str],
        name: str | None = None,
        n_jobs: int = 1,
        verbose: bool = False,
    ) -> None:
        self._columns = tuple(columns)  # fixed param, set BEFORE super().__init__()
        super().__init__(
            name=name or f"ColumnSubset({len(self._columns)} cols)",
            n_jobs=n_jobs,
            verbose=verbose,
        )

    @staticmethod
    def ts_transform(series: TimeSeries, params: dict) -> TimeSeries:
        cols = list(params["fixed"]["_columns"])
        missing = set(cols) - set(series.components)
        if missing:
            raise ValueError(f"ColumnSubset: components not present: {sorted(missing)}")
        return series[cols]  # component selection preserves freq/index
