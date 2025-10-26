import os
import subprocess as sp
import time
import pytest
import requests  # Used for checking service readiness


@pytest.fixture(scope="session", autouse=True)
def live_servers():
    """Launches FastAPI and Streamlit in parallel processes."""

    # Configure environment variables for test
    FASTAPI_PORT = 8000
    STREAMLIT_PORT = 8501
    os.environ["DATA_DIR"] = "data_tests"

    # FastApi
    fastapi_command = [
        "uvicorn",
        "src.api.main:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(FASTAPI_PORT),
    ]
    fastapi_process = sp.Popen(fastapi_command)
    print(f"FastAPI process started on port {FASTAPI_PORT} with PID {fastapi_process.pid}")

    # Streamlit
    streamlit_command = [
        "streamlit",
        "run",
        "src/frontend/streamlit.py",
        "--server.port",
        str(STREAMLIT_PORT),
        "--server.headless",
        "true",  # prevent Streamlit from launching a browser
    ]
    streamlit_process = sp.Popen(streamlit_command)
    print(f"Streamlit process started on port {STREAMLIT_PORT} with PID {streamlit_process.pid}")

    _wait_for_server(f"http://127.0.0.1:{FASTAPI_PORT}/docs", "FastAPI")
    _wait_for_server(f"http://127.0.0.1:{STREAMLIT_PORT}", "Streamlit")

    # The yield passes control to the tests, and the code after yield runs as teardown
    yield {
        "fastapi_url": f"http://127.0.0.1:{FASTAPI_PORT}",
        "streamlit_url": f"http://127.0.0.1:{STREAMLIT_PORT}",
    }

    fastapi_process.terminate()
    streamlit_process.terminate()

    fastapi_process.wait(timeout=5)
    streamlit_process.wait(timeout=5)


def _wait_for_server(url, service_name, timeout=30, poll_interval=0.5):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code < 500:
                return
        except requests.exceptions.ConnectionError:
            pass
        except Exception as e:
            print(f"Error checking {service_name} status: {e}")
        time.sleep(poll_interval)

    raise TimeoutError(f"Timed out waiting for {service_name} to start at {url}")
