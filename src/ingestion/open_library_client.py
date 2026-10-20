"""HTTP client for the public Open Library Search API (https://openlibrary.org/dev/docs/api/search)."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SEARCH_PATH = "/search.json"
# Request only the fields the pipeline uses; keeps responses small and polite.
SEARCH_FIELDS = "key,title,author_name,isbn,first_publish_year,edition_count"
RETRY_STATUSES = (429, 500, 502, 503, 504)


class OpenLibraryError(RuntimeError):
    """Raised when a request fails permanently (after retries) or returns an unusable body."""


@dataclass(frozen=True)
class ApiResponse:
    url: str
    params: dict[str, str]
    status_code: int
    body: bytes
    fetched_at: datetime
    elapsed_ms: int
    record_count: int


class OpenLibraryClient:
    def __init__(
        self,
        base_url: str,
        user_agent: str,
        timeout_seconds: float,
        max_retries: int,
        backoff_factor: float = 0.5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=RETRY_STATUSES,
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
            # Return the final response instead of raising, so we can report its status.
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent, "Accept": "application/json"})
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def __enter__(self) -> OpenLibraryClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        self.session.close()

    def search_by_subject(self, subject: str, limit: int) -> ApiResponse:
        params = {
            "subject": subject.replace("_", " "),
            "fields": SEARCH_FIELDS,
            "limit": str(limit),
        }
        url = f"{self.base_url}{SEARCH_PATH}"
        started = time.monotonic()
        try:
            response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        except requests.RequestException as exc:
            raise OpenLibraryError(f"request to {url} failed after retries: {exc}") from exc
        elapsed_ms = int((time.monotonic() - started) * 1000)

        if response.status_code != 200:
            raise OpenLibraryError(
                f"Open Library returned HTTP {response.status_code} for subject={subject!r}"
            )
        try:
            payload = json.loads(response.content)
        except ValueError as exc:
            raise OpenLibraryError(f"response for subject={subject!r} is not valid JSON") from exc
        docs = payload.get("docs") if isinstance(payload, dict) else None
        if not isinstance(docs, list):
            raise OpenLibraryError(f"response for subject={subject!r} has no 'docs' array")

        return ApiResponse(
            url=response.url,
            params=params,
            status_code=response.status_code,
            body=response.content,
            fetched_at=datetime.now(UTC),
            elapsed_ms=elapsed_ms,
            record_count=len(docs),
        )
