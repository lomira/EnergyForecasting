from datetime import datetime

import pandas as pd
import pandera.pandas as pa


# LOAD MODEL
class HolidaysSchema(pa.DataFrameModel):
    datetime: datetime = pa.Field(description="PRIMARY_KEY")
    holidays: bool = pa.Field(
        description="Indicates whether the date is a public holiday or not"
    )

    @pa.check("datetime")
    def check_chronological(cls, s: pd.Series) -> bool:
        # Returns True if the entire series is chronological
        return s.is_monotonic_increasing and s.is_unique

    @pa.check("datetime")
    def check_tz_naive(cls, s: pd.Series) -> bool:
        # Returns True if the entire series is timezone naive
        return s.dt.tz is None
