import openmeteo_requests
import requests_cache
from retry_requests import retry
from src.data.schemas.openmeteo_api import OpenMeteoRequest


def create_openmeteo_client() -> openmeteo_requests.Client:
    """Create and return an OpenMeteo client with retry logic."""
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo_client = openmeteo_requests.Client(session=retry_session)  # ty: ignore[invalid-argument-type]
    return openmeteo_client


def fetch_openmeteo_data_from_request(client: openmeteo_requests.Client, request: OpenMeteoRequest):
    """Make a request to the OpenMeteo API and return the response."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = request.model_dump(exclude_none=True)
    responses = client.weather_api(url, params=params)
    return responses[0]  # Return the first location (assuming single location request)
