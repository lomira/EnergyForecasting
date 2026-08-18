"""Bridge between the data packages and Darts TimeSeries."""

import pandas as pd
from darts import TimeSeries

from engine.ingestion.internal_db import (
    read_corrected_load,
    read_future_covariates,
)


def get_load_ts(
    from_date: pd.Timestamp | None = None,
    to_date: pd.Timestamp | None = None,
) -> TimeSeries:
    """Build a univariate Darts TimeSeries from the load-data store.

    Parameters
    ----------
    from_date, to_date : pd.Timestamp, optional
        Filter range. If None, uses the full table.
    """

    return TimeSeries.from_dataframe(
        read_corrected_load(from_date, to_date), value_cols="load_mw"
    )


def get_covariate_ts(
    from_date: pd.Timestamp,
    to_date: pd.Timestamp,
) -> TimeSeries:
    """Build a multivariate Darts TimeSeries from the covariate store.

    Parameters
    ----------
    from_date, to_date : pd.Timestamp
        Date range for covariates.
    """
    return TimeSeries.from_dataframe(read_future_covariates(from_date, to_date))
