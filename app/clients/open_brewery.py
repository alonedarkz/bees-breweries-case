from __future__ import annotations

from dataclasses import dataclass
from time import sleep
from typing import Any

import requests

from app.config import DEFAULT_PAGE_SIZE, DEFAULT_REQUEST_TIMEOUT, OPEN_BREWERY_API_URL


@dataclass
class BreweryAPIClient:
    base_url: str = OPEN_BREWERY_API_URL
    per_page: int = DEFAULT_PAGE_SIZE
    timeout: int = DEFAULT_REQUEST_TIMEOUT
    max_retries: int = 3
    backoff_seconds: int = 2

    def fetch_all(self) -> list[dict[str, Any]]:
        breweries: list[dict[str, Any]] = []
        page = 1

        while True:
            payload = self._request_page(page)
            if not payload:
                break

            breweries.extend(payload)
            page += 1

        return breweries

    def _request_page(self, page: int) -> list[dict[str, Any]]:
        params = {"page": page, "per_page": self.per_page}

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(self.base_url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.RequestException:
                if attempt == self.max_retries:
                    raise
                sleep(self.backoff_seconds * attempt)

        return []
