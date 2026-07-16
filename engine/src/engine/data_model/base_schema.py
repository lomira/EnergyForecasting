from datetime import datetime

import pandas as pd
import pandera.pandas as pa


class BaseTimeSeriesSchema(pa.DataFrameModel):
    datetime: datetime = pa.Field(description="PRIMARY_KEY")

    @pa.check("datetime")
    def check_chronological(cls, s: pd.Series) -> bool:
        # Returns True if the entire series is chronological
        return s.is_monotonic_increasing and s.is_unique

    @pa.check("datetime")
    def check_tz_naive(cls, s: pd.Series) -> bool:
        # Returns True if the entire series is timezone naive
        return s.dt.tz is None
