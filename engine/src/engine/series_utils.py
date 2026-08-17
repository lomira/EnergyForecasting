"""Bridge between the data packages and Darts TimeSeries."""

import pandas as pd
from darts import TimeSeries

from holiday_data import read as read_holidays
from load_data import read as read_load
from weather_data import WEATHER_PREVIOUS_DAYS
from weather_data import read as read_weather


def _covariates_dataframe(
    from_date: pd.Timestamp,
    to_date: pd.Timestamp,
) -> pd.DataFrame:
    df = pd.concat(
        [read_weather(from_date, to_date), read_holidays(from_date, to_date)],
        axis=1,
        join="outer",
    )
    if df.empty:
        raise ValueError(f"No covariates found between {from_date} and {to_date}")
    return df


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
    from_date: pd.Timestamp,
    to_date: pd.Timestamp,
    feature_subset: tuple[str, ...] | None = None,
) -> TimeSeries:
    """Build a multivariate Darts TimeSeries from the covariate store.

    Parameters
    ----------
    from_date, to_date : pd.Timestamp
        Date range for covariates.
    feature_subset : tuple of str, optional
        If provided, only these columns are included. This enables the model
        config's ``feature_subset`` to be applied at the covariate construction
        stage rather than inside the pipeline.
    """

    df = _covariates_dataframe(from_date, to_date)

    if feature_subset:
        missing = set(feature_subset) - set(df.columns)
        if missing:
            raise ValueError(
                f"Feature subset columns not found in covariates: {sorted(missing)}"
            )
        df = df[list(feature_subset)]

    return TimeSeries.from_dataframe(df)


def forecast_covariates_time_series(
    from_date: pd.Timestamp,
    to_date: pd.Timestamp,
    forecast_start: pd.Timestamp,
    feature_subset: tuple[str, ...],
) -> TimeSeries:
    """Build covariates using only weather forecasts available at prediction time."""
    if not from_date <= forecast_start <= to_date:
        raise ValueError("forecast_start must be within the covariate range")
    if not feature_subset:
        raise ValueError("feature_subset must contain at least one feature")

    df = _covariates_dataframe(from_date, to_date)
    missing = set(feature_subset) - set(df.columns)
    if missing:
        raise ValueError(
            f"Feature subset columns not found in covariates: {sorted(missing)}"
        )

    forecast_rows = df.index >= forecast_start
    lead_days = (df.index[forecast_rows] - forecast_start) // pd.Timedelta(days=1) + 1
    if len(lead_days) and max(lead_days) > WEATHER_PREVIOUS_DAYS:
        raise ValueError(
            f"Weather forecasts are stored for only {WEATHER_PREVIOUS_DAYS} days"
        )

    for feature in feature_subset:
        first_vintage = f"{feature}_previous_day1"
        if feature == "holidays":
            continue
        if first_vintage not in df.columns:
            raise ValueError(f"Missing forecast-vintage column: {first_vintage}")
        for day in range(1, WEATHER_PREVIOUS_DAYS + 1):
            vintage = f"{feature}_previous_day{day}"
            if vintage not in df.columns:
                raise ValueError(f"Missing forecast-vintage column: {vintage}")
            day_rows = df.index[forecast_rows][lead_days == day]
            df.loc[day_rows, feature] = df.loc[day_rows, vintage]

    selected = df[list(feature_subset)]
    missing_forecast = selected.loc[forecast_rows].columns[
        selected.loc[forecast_rows].isna().any()
    ]
    if len(missing_forecast):
        raise ValueError(
            "Missing forecast-vintage values for: "
            + ", ".join(map(str, missing_forecast))
        )
    return TimeSeries.from_dataframe(selected)
