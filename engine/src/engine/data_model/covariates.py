import pandera.pandas as pa

from engine.data_model.base_schema import BaseTimeSeriesSchema


# LOAD MODEL
class CovariatesSchema(BaseTimeSeriesSchema):
    any_column: float = pa.Field(alias=".*", regex=True)
