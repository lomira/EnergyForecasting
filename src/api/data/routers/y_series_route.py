from typing import Annotated

from fastapi import APIRouter, File, Form, UploadFile

from src.api.data.schemas.y_series_api import APITimeSeriesInput
from src.config import get_settings

router = APIRouter(prefix="/ingest", tags=["ingestion"])


@router.post("/y-series-csv")
async def upload_y_series_csv(
    file: Annotated[UploadFile, File(...)],
    granularity: Annotated[str, Form(...)],
    timezone: Annotated[str, Form(...)],
):
    """Endpoint to upload Y series CSV data."""
    content = await file.read()
    raw_data = (content.decode("utf-8"), granularity, timezone)
    api_input = APITimeSeriesInput.from_api_data(raw_data)
    timeseries_data = api_input.to_timeseries()
    timeseries_data_csv = timeseries_data.to_csv()

    # Verify file written to disk
    settings = get_settings()
    file_path = settings.data_dir / settings.raw_dir / f"y_{timeseries_data.granularity}.csv"
    file_written = file_path.exists()

    return {
        "message": "Y series data uploaded successfully.",
        "data_points": len(timeseries_data.dataframe),
        "csv": timeseries_data_csv,
        "file_path": str(file_path),
        "file_written": file_written,
    }
