"""Simple future-covariate scenario generation."""

import numpy as np
import pandas as pd
from darts import TimeSeries


def random_future_scenario(
    variables: list[str],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> TimeSeries:
    """Return inclusive hourly random values for the requested variables."""
    index = pd.date_range(start_date, end_date, freq="h", name="datetime")
    df = pd.DataFrame(
        np.random.default_rng().random((len(index), len(variables))),
        index=index,
        columns=variables,
    )
    return TimeSeries.from_dataframe(df)
