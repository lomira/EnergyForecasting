"""Holiday feature engineering."""

import pandas as pd

import holiday_data
from engine.featurize.features import Feature


def holiday_features(
    from_date: pd.Timestamp, to_date: pd.Timestamp
) -> pd.DataFrame:
    holidays = holiday_data.read(from_date, to_date)
    index = pd.date_range(
        start=from_date.floor("d"),
        end=to_date,
        freq="h",
    )
    holidays = pd.DataFrame(
        holidays.reindex(index, method="ffill").fillna(0),
        columns=[Feature.HOLIDAYS.value],
    )
    return holidays[holidays.index >= from_date]
