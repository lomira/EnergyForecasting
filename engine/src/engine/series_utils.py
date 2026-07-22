"""Bridge between Django ORM (ingestion) and Darts TimeSeries"""

from datetime import datetime

import pandas as pd
from darts import TimeSeries

from engine.ingestion.get_all_covariates import get_all_covariates
from engine.models import LoadObservation


def load_time_series(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
) -> TimeSeries:
    """Build a univariate Darts TimeSeries from ``LoadObservation``.

    Parameters
    ----------
    from_date, to_date : datetime, optional
        Filter range. If None, uses the full table.
    """

    # Using queryset to optimize memony and not useless data from the database.
    queryset = LoadObservation.objects.all().order_by("datetime")
    if from_date is not None:
        queryset = queryset.filter(datetime__gte=from_date)
    if to_date is not None:
        queryset = queryset.filter(datetime__lte=to_date)

    records = list(queryset.values("datetime", "load_mw"))
    if not records:
        raise ValueError("No load observations found in the database.")

    df = pd.DataFrame.from_records(records).set_index("datetime").sort_index()
    return TimeSeries.from_dataframe(df, time_col="datetime", value_cols="load_mw")


def covariates_time_series(
    from_date: datetime,
    to_date: datetime,
    feature_subset: tuple[str] = (),
) -> TimeSeries:
    """Build a multivariate Darts TimeSeries from the covariate store.

    Parameters
    ----------
    from_date, to_date : datetime
        Date range for covariates.
    feature_subset : tuple of str, optional
        If provided, only these columns are included. This enables the model
        config's ``feature_subset`` to be applied at the covariate construction
        stage rather than inside the pipeline.
    """
    df = get_all_covariates(from_date, to_date)
    if feature_subset:
        missing = set(feature_subset) - set(df.columns)
        if missing:
            raise ValueError(
                f"Feature subset columns not found in covariates: {sorted(missing)}"
            )
        df = df[list(feature_subset)]

    return TimeSeries.from_dataframe(df, time_col="datetime")


def build_training_data(
    from_date: datetime,
    to_date: datetime,
    feature_subset: tuple[str, ...] = (),
) -> tuple[TimeSeries, TimeSeries | None]:
    """One-stop shop: load target + covariates as Darts TimeSeries.

    Returns
    -------
    (series, future_cov)
        Covariates are returned as **future covariates** by default
        They are known at prediction time : eg weather forecasts, timeattribute, etc.
    """
    series = load_time_series(from_date, to_date)
    future_cov = covariates_time_series(from_date, to_date, feature_subset)
    return series, future_cov
