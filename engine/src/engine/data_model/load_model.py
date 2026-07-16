import pandera.pandas as pa

from engine.data_model.base_schema import BaseTimeSeriesSchema


# LOAD MODEL
class LoadSchema(BaseTimeSeriesSchema):
    load_MW: float = pa.Field(ge=0)
