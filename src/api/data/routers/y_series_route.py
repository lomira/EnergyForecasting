from fastapi import APIRouter, File, Form, UploadFile
from src.api.data.schemas.y_series_api import APITimeSeriesInput

router = APIRouter(prefix="/ingest", tags=["ingestion"])


@router.post("/y-series-csv")
async def upload_y_series_csv(
    file: UploadFile = File(...),
    granularity: str = Form(...),
    timezone: str = Form(...),
):
    """Endpoint to upload Y series CSV data."""
    content = await file.read()
    raw_data = (content.decode("utf-8"), granularity, timezone)
    api_input = APITimeSeriesInput.from_api_data(raw_data)
    timeseries_data = api_input.to_timeseries()
    timeseries_data_csv = timeseries_data.to_csv()
    return {
        "message": "Y series data uploaded successfully.",
        "data_points": len(timeseries_data.dataframe),
        "csv": timeseries_data_csv,
    }
