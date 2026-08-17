"""Bridge between the data packages and Darts TimeSeries."""

import pandas as pd
from darts import TimeSeries

from holiday_data import read as read_holidays
from load_data import read as read_load
from weather_data import read as read_weather


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
        read_load(from_date, to_date), value_cols="load_mw"
    )


def covariates_time_series(
    from_date: pd.Timestamp | int,
    to_date: pd.Timestamp | int,
    feature_subset: tuple[str] | None = None,
) -> TimeSeries:
    """Build a multivariate Darts TimeSeries from the covariate store.

    Parameters
    ----------
    from_date, to_date : pd.Timestamp or int
        Date range for covariates.
    feature_subset : tuple of str, optional
        If provided, only these columns are included. This enables the model
        config's ``feature_subset`` to be applied at the covariate construction
        stage rather than inside the pipeline.
    """

    df = pd.concat(
        [read_weather(from_date, to_date), read_holidays(from_date, to_date)],  # ty: ignore[invalid-argument-type]
        axis=1,
        join="outer",
    )
    if df.empty:
        raise ValueError(f"No covariates found between {from_date} and {to_date}")

    if feature_subset:
        missing = set(feature_subset) - set(df.columns)
        if missing:
            raise ValueError(
                f"Feature subset columns not found in covariates: {sorted(missing)}"
            )
        df = df[list(feature_subset)]

    return TimeSeries.from_dataframe(df)
