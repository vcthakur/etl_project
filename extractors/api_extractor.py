import requests
import pandas as pd

class APIExtractor:
    def __init__(self, config: dict):
        """
        Initialize the API extractor.
        config example:
        {
            "url": "http://127.0.0.1:5001/data",
            "method": "GET",
            "headers": {"Authorization": "Bearer xyz"},
            "params": {"limit": 100},
            "json_key": None   # optional: if JSON response has nested data
        }
        """
        self.url = config.get("url")
        self.method = config.get("method", "GET").upper()
        self.headers = config.get("headers", {})
        self.params = config.get("params", {})
        self.json_key = config.get("json_key", None)

    def extract(self) -> pd.DataFrame:
        """Extract data from API and return as pandas DataFrame."""
        response = requests.request(
            method=self.method,
            url=self.url,
            headers=self.headers,
            params=self.params
        )

        # Raise an error if request failed
        response.raise_for_status()

        data = response.json()

        # If API response has nested structure, allow extracting from a key
        if self.json_key:
            data = data.get(self.json_key, [])

        # Convert list/dict to DataFrame safely
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            return pd.DataFrame([data])
        else:
            raise ValueError("Unexpected API response format.")
