from typing import Annotated

from fastapi import APIRouter, File, Form, UploadFile

from src.api.data.schemas.timeseries_api import APITimeSeriesInput
from src.config import get_settings

router = APIRouter(prefix="/ingest", tags=["ingestion"])


@router.post("/timeseries-csv")
async def upload_timeseries_csv(
    file: Annotated[UploadFile, File(...)],
    name: Annotated[str, Form(...)],
    granularity: Annotated[str, Form(...)],
    timezone: Annotated[str, Form(...)],
):
    """Endpoint to upload timeseries CSV data."""
    content = await file.read()
    raw_data = (content.decode("utf-8"), name, granularity, timezone)
    api_input = APITimeSeriesInput.from_api_data(raw_data)
    timeseries_data = api_input.to_timeseries()
    timeseries_data_csv = timeseries_data.to_csv()

    # Verify file written to disk
    settings = get_settings()
    # TODO : ensure consistency with the filename used in to_csv method in timeseries.py
    file_path = (
        settings.data_dir
        / settings.raw_dir
        / f"{timeseries_data.name}_{timeseries_data.granularity}.csv"
    )

    file_written = file_path.exists()

    return {
        "message": "Timeseries data uploaded successfully.",
        "data_points": len(timeseries_data.dataframe),
        "csv": timeseries_data_csv,
        "file_path": str(file_path),
        "file_written": file_written,
    }
