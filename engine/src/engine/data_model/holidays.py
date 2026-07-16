import pandera.pandas as pa

from engine.data_model.base_schema import BaseTimeSeriesSchema


# LOAD MODEL
class HolidaysSchema(BaseTimeSeriesSchema):
    holidays: bool = pa.Field(
        description="Indicates whether the date is a public holiday or not"
    )
