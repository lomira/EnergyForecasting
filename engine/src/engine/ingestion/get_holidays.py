from datetime import datetime

import holidays
import pandas as pd

from engine.models import Holiday


def get_holidays(from_date: datetime, to_date: datetime) -> None:
    """Insert the public holidays for a timerange into the database"""
    dz_holidays = holidays.country_holidays(
        "DZ", years=range(from_date.year, to_date.year + 1)
    )

    date_range = pd.date_range(start=from_date, end=to_date, freq="h")
    holidays_df = pd.DataFrame(index=date_range)

    # ci_holidays maps date -> name; build a DatetimeIndex of holiday dates
    # and test membership against the (hourly) frame index.
    holiday_dates = pd.to_datetime(list(dz_holidays.keys()))
    holidays_df["is_holiday"] = holidays_df.index.isin(holiday_dates).astype(int)
    # put the index as datetime
    holidays_df = holidays_df.rename_axis("datetime").reset_index()

    observations = [
        Holiday(datetime=row["datetime"], is_holiday=bool(row["is_holiday"]))
        for row in holidays_df.to_dict("records")
    ]
    Holiday.objects.bulk_create(
        observations,
        update_conflicts=True,
        unique_fields=["datetime"],
        update_fields=["is_holiday"],
    )
