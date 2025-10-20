import requests
from src.data.schemas.openmeteo_api import GeocodingAPIRequest


def create_geocoding_client() -> requests.Session:
    """Create and return a geocoding client with retry logic."""
    BASE_URL = "https://geocoding-api.open-meteo.com/v1/search"
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    session.base_url = BASE_URL  # ty: ignore[unresolved-attribute]
    return session


def fetch_geocoding_data(client: requests.Session, query: GeocodingAPIRequest):
    """Make a request to the OpenMeteo Geocoding API and return the response."""
    params = {
        "name": query.name,
        "count": 1,
        "countryCode": query.country,
        "language": "en",
        "format": "json",
    }
    response = client.get(client.base_url, params=params)  # ty: ignore[unresolved-attribute]
    response.raise_for_status()
    return response.json().get("results")[0]
