import json
import requests
from src.data.schemas.openmeteo_api import GeocodingAPIRequest, GeocodingAPIResponse
from src.config import get_settings

SETTINGS = get_settings()


def create_geocoding_client() -> requests.Session:
    """Create and return a geocoding client with retry logic."""
    BASE_URL = "https://geocoding-api.open-meteo.com/v1/search"
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    session.base_url = BASE_URL  # ty: ignore[unresolved-attribute]
    return session


def _fetch_geocoding_data(client: requests.Session, query: GeocodingAPIRequest):
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


def geocode_city(client: requests.Session, query: GeocodingAPIRequest) -> GeocodingAPIResponse:
    """Fetch geocoding data for a given city and country code."""
    weather_dir = SETTINGS.full_weather_dir
    # If a file with the city name exists in the weather_dir, read from it
    import os

    file_path = os.path.join(weather_dir, f"{query.name}_{query.country}_geocode.json")
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            data = json.load(f)
        try:
            return GeocodingAPIResponse.from_api_response(data)
        except Exception as e:
            print(f"Error loading cached geocoding data: {e}")
            pass

    data = _fetch_geocoding_data(client, query)
    api_respone = GeocodingAPIResponse.from_api_response(data)
    json_data = api_respone.model_dump_json()

    # Save the response to a file for caching
    with open(file_path, "w") as f:
        f.write(json_data)
    return GeocodingAPIResponse.from_api_response(data)
