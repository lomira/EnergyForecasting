import sys
from pathlib import Path

import requests
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_settings  # noqa:E402

SETTINGS = get_settings()


def _api_post_csv(csv_bytes: str, name: str, granularity: str, timezone: str):
    """Helper to post CSV data to API and get response."""

    api_url = f"{SETTINGS.api_base}/ingest/timeseries-csv"

    files = {
        "file": ("data.csv", csv_bytes, "text/csv"),
    }
    data = {
        "name": name,
        "granularity": granularity,
        "timezone": timezone,
    }

    response = requests.post(api_url, files=files, data=data)
    response.raise_for_status()
    return response.json()


def page_upload():
    st.title("Upload Time Series Data")

    uploaded_file = st.file_uploader(
        label="Upload a Timeseries.",
        type="csv",
        accept_multiple_files=False,
        key="timeseries_uploader",
        help="The CSV file should have two columns: timestamp and value.",
    )

    name = st.text_input(
        label="Series Name",
        value="Load Curve",
        key="timeserie_name",
        help="A name to identify the time series.",
    )
    granularity = st.selectbox(
        label="Granularity",
        options=SETTINGS.granularity_options,
        index=0,
        key="timeserie_granularity",
        help="The time interval between data points.",
    )

    timezone = st.selectbox(
        label="Timezone",
        options=SETTINGS.allowed_timezones,
        index=0,
        key="timeserie_timezone",
        help="The timezone of the timestamps in the data.",
    )

    button_clicked = st.button(
        label="Upload", key="timeseries_upload_btn", disabled=not uploaded_file
    )

    if uploaded_file and button_clicked:
        try:
            csv_text = uploaded_file.getvalue()
            api_response = _api_post_csv(csv_text, name, granularity, timezone)
            st.success("File uploaded and processed successfully!")
            st.json(api_response)
        except Exception as e:
            st.error(f"Error processing file: {e}")


PAGES = {
    "Upload": page_upload,
}


def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Page", list(PAGES.keys()))
    PAGES[page]()


if __name__ == "__main__":
    main()
