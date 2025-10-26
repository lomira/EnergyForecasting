from playwright.sync_api import Page, expect
import os


def test_streamlit_integration(page: Page, live_servers):
    streamlit_url = live_servers["streamlit_url"]

    # Double-check that the environment variable is set correctly
    assert os.environ["DATA_DIR"] == "data_tests"

    # Tests
    page.goto(streamlit_url)
    expect(page.get_by_text("Upload Time Series Data")).to_be_visible()
