from pathlib import Path
import io
import pandas as pd
from fastapi.testclient import TestClient
from src.api.main import app
from src.api.data.schemas.timeseries_api import GRANULARITY_FREQ_MAP, ALLOWED_TIMEZONES

from src.config import get_settings

client = TestClient(app)

SETTINGS = get_settings()


def test_upload_timeseries_csv_endpoint_returns_csv_and_count():
    name = "abc_series"
    granularity = next(iter(GRANULARITY_FREQ_MAP.keys()))
    timezone = next(iter(ALLOWED_TIMEZONES))

    csv_text = """
        timestamp, value
        2023-01-01 00:00:00+00:00, 10
        2023-01-02 00:00:00+00:00, 20
        2023-01-03 00:00:00+00:00, 30
        """
    file_bytes = csv_text.encode("utf-8")

    files = {
        "file": ("y.csv", io.BytesIO(file_bytes), "text/csv"),
    }
    data = {
        "name": name,
        "granularity": granularity,
        "timezone": timezone,
    }

    # Override application settings so CSV is written into a test-specific folder
    import os

    os.environ["DATA_DIR"] = "data_test"

    # Ensure clean test target directory and remove any pre-existing file
    target_dir = Path(os.environ["DATA_DIR"]) / SETTINGS.raw_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / f"{name}_{granularity}.csv"
    if target_file.exists():
        target_file.unlink()

    resp = client.post("/ingest/timeseries-csv", files=files, data=data)
    assert resp.status_code == 200, resp.text

    payload = resp.json()
    # verify API reports file written and that file exists on disk
    assert payload.get("file_written") is True
    file_path = payload.get("file_path")
    assert file_path is not None
    import os

    assert os.path.exists(file_path), f"Expected file at {file_path}"
    assert "message" in payload and "data_points" in payload and "csv" in payload

    assert payload["data_points"] == 3
    returned_df = pd.read_csv(io.StringIO(payload["csv"]))
    assert len(returned_df) == 3
