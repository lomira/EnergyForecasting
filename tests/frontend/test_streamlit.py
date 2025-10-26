from playwright.sync_api import Page, expect
import os
from pathlib import Path
import time
import warnings
from hypothesis.errors import NonInteractiveExampleWarning
from tests.strategies.timeseries_data_strategy import timeseries_data_strategy


def test_streamlit_integration(page: Page, live_servers):
    streamlit_url = live_servers["streamlit_url"]

    # Double-check that the environment variable is set correctly
    assert os.environ["DATA_DIR"] == "data_tests"

    # Tests
    page.goto(streamlit_url)

    # Verify page title
    title_page = page.get_by_text("Upload Time Series Data")
    expect(title_page).to_be_visible()

    # Verify upload button is disabled initially
    upload_button = page.get_by_role("button", name="Upload")
    expect(upload_button).to_be_disabled()

    # Fill in the name field
    warnings.filterwarnings("ignore", category=NonInteractiveExampleWarning)
    ts_data_payload = timeseries_data_strategy().example()
    name_input = page.get_by_label("Series Name")
    name_input.fill(ts_data_payload["name"])
    expect(name_input).to_have_value(ts_data_payload["name"])
    # Upload the CSV file
    csv_to_input = {
        "name": ts_data_payload["name"][::-1] + ".csv",
        "mimeType": "text/csv",
        "buffer": ts_data_payload["csv_text"].encode(),
    }
    file_input = page.locator('input[type="file"]')
    file_input.set_input_files(csv_to_input)
    # Verify upload button is enabled after file upload
    expect(upload_button).to_be_enabled()

    upload_button.click()
    time.sleep(10)  # Wait for upload to process #TODO optimize this wait
    saved_path = (
        Path(os.environ["DATA_DIR"]) / Path("raw") / f"{ts_data_payload['name']}_hourly.csv"
    )
    file_exists = os.path.exists(saved_path)
    assert file_exists
    # Click the upload button
    time.sleep(20)  # Wait for upload to process
