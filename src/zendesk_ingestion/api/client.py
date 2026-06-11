"""Zendesk REST API client with retries, pagination, and rate limiting."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import httpx
import structlog
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from zendesk_ingestion.api.pagination import CursorPage, OffsetPage
from zendesk_ingestion.api.rate_limiter import RateLimiter
from zendesk_ingestion.exceptions import ZendeskAPIError, ZendeskRateLimitError

log = structlog.get_logger()


def _wait_from_retry_after(retry_state: RetryCallState) -> float:
    """tenacity wait function that reads retry_after from a ZendeskRateLimitError."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(exc, ZendeskRateLimitError):
        return float(exc.retry_after)
    return 1.0


class ZendeskClient:
    def __init__(
        self,
        subdomain: str,
        email: str,
        api_token: str,
        rate_limiter: RateLimiter,
        base_url: str | None = None,
    ) -> None:
        self._subdomain = subdomain
        self._email = email
        self._api_token = api_token
        self._rate_limiter = rate_limiter
        self._base_url = base_url or f"https://{subdomain}.zendesk.com"
        self._client = httpx.Client(
            base_url=self._base_url,
            auth=(f"{email}/token", api_token),
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(max_keepalive_connections=10),
        )
        self.last_cursor: str | None = None

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> ZendeskClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # ----- low-level request with retries --------------------------------

    def _request(self, path_or_url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a single GET with rate limiting and retry. Returns parsed JSON."""

        @retry(
            retry=retry_if_exception_type(ZendeskRateLimitError),
            wait=_wait_from_retry_after,
            stop=stop_after_attempt(5),
            reraise=True,
        )
        @retry(
            retry=retry_if_exception_type((httpx.TransportError, _ServerError)),
            wait=wait_exponential_jitter(initial=2.0, max=60.0),
            stop=stop_after_attempt(5),
            reraise=True,
        )
        def _do() -> dict[str, Any]:
            self._rate_limiter.acquire()
            if path_or_url.startswith("http"):
                response = self._client.get(path_or_url, params=params)
            else:
                response = self._client.get(path_or_url, params=params)
            return _parse_response(response)

        return _do()

    # ----- offset/link pagination ----------------------------------------

    def get_page(self, path: str, params: dict[str, Any] | None = None) -> OffsetPage:
        """
        Single GET request. Returns one OffsetPage (records + next_url).
        Raises ZendeskRateLimitError on 429, ZendeskAPIError on other 4xx/5xx.
        """
        body = self._request(path, params)
        records = _extract_records(body)
        next_url = body.get("next_page")
        return OffsetPage(records=records, next_url=next_url)

    def paginate(self, path: str, params: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        """Yield all records across all pages for a standard list endpoint."""
        page = self.get_page(path, params=params)
        yield from page.records
        while page.next_url:
            page = self.get_page(page.next_url)
            yield from page.records

    # ----- cursor-based incremental pagination ---------------------------

    def get_incremental_cursor(self, path: str, cursor: str | None = None) -> CursorPage:
        """
        Single call to a Zendesk incremental cursor endpoint.
        Sets end_of_stream=True when response has 'end_of_stream: true'.
        """
        params: dict[str, Any] = {}
        if cursor:
            params["cursor"] = cursor
        body = self._request(path, params)
        records = _extract_records(body)
        next_cursor = body.get("after_cursor")
        end_of_stream = bool(body.get("end_of_stream", False))
        return CursorPage(
            records=records,
            next_cursor=next_cursor,
            end_of_stream=end_of_stream,
        )

    def paginate_incremental(
        self, path: str, cursor: str | None = None
    ) -> Iterator[dict[str, Any]]:
        """
        Yield all records from an incremental cursor endpoint.
        Stops when end_of_stream is True. Final cursor available via self.last_cursor.
        """
        current_cursor = cursor
        while True:
            page = self.get_incremental_cursor(path, current_cursor)
            if page.next_cursor is not None:
                self.last_cursor = page.next_cursor
            yield from page.records
            if page.end_of_stream:
                return
            if page.next_cursor is None:
                return
            current_cursor = page.next_cursor


class _ServerError(Exception):
    """Internal marker for 5xx responses — triggers tenacity retry."""


def _parse_response(response: httpx.Response) -> dict[str, Any]:
    """Convert HTTP status to the appropriate exception or return JSON body."""
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", "60"))
        raise ZendeskRateLimitError(retry_after=retry_after)
    if 500 <= response.status_code < 600:
        raise _ServerError(f"{response.status_code}: {response.text[:200]}")
    if 400 <= response.status_code < 500:
        raise ZendeskAPIError(response.status_code, response.text[:500])
    return response.json()  # type: ignore[no-any-return]


def _extract_records(body: dict[str, Any]) -> list[dict[str, Any]]:
    """Find the records list inside a Zendesk response body.

    Zendesk returns records under a plural key matching the resource. We pick the
    first list value in the response that isn't a meta field.
    """
    meta_keys = {
        "next_page",
        "previous_page",
        "count",
        "after_cursor",
        "before_cursor",
        "after_url",
        "before_url",
        "end_of_stream",
        "end_time",
        "links",
        "meta",
    }
    for key, value in body.items():
        if key in meta_keys:
            continue
        if isinstance(value, list):
            return value
    return []
