import requests
from typing import Dict, Any

class APIExtractor:
    def __init__(self, base_url: str, headers: Dict[str, str] = None, timeout: int = 30):
        self.base_url = base_url
        self.headers = headers or {}
        self.timeout = timeout
        self.session = requests.Session()

    def connect(self) -> requests.Session:
        self.session.headers.update(self.headers)
        return self.session

    def extract_data(self, endpoint: str, params: Dict[str, Any] = None) -> Any:
        url = f"{self.base_url}{endpoint}"
        response = self.session.get(url, params=params, timeout=self.timeout)

        if response.status_code != 200:
            response.raise_for_status()

        return response.json()

    def close_connection(self) -> None:
        self.session.close()
