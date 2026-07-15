from datetime import datetime

import holidays
import pandas as pd


# Fetch Côte d'Ivoire holidays for a certain time
def get_holidays(from_date: datetime, to_date: datetime) -> pd.DataFrame:
    ci_holidays = holidays.country_holidays(
        "CI", years=range(from_date.year, to_date.year + 1)
    )

    date_range = pd.date_range(start=from_date, end=to_date, freq="h")
    holidays_df = pd.DataFrame(index=date_range)

    holidays_df["holidays"] = holidays_df.index.date
    holidays_df["holidays"] = holidays_df["holidays"].apply(
        lambda x: 1 if x in ci_holidays else 0
    )

    return holidays_df
