from __future__ import annotations

from typing import Any

import httpx
import pytest

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.api.rate_limiter import RateLimiter
from zendesk_ingestion.exceptions import ZendeskAPIError


def _make_client(handler: httpx.MockTransport) -> ZendeskClient:
    rate_limiter = RateLimiter(requests_per_minute=6000)
    client = ZendeskClient(
        subdomain="acme",
        email="user@example.com",
        api_token="token",
        rate_limiter=rate_limiter,
    )
    client._client = httpx.Client(  # type: ignore[assignment]
        base_url="https://acme.zendesk.com",
        transport=handler,
        timeout=httpx.Timeout(30.0),
    )
    return client


def test_paginate_follows_next_links() -> None:
    pages = [
        {
            "users": [{"id": 1}, {"id": 2}],
            "next_page": "https://acme.zendesk.com/api/v2/users.json?page=2",
        },
        {
            "users": [{"id": 3}, {"id": 4}],
            "next_page": "https://acme.zendesk.com/api/v2/users.json?page=3",
        },
        {"users": [{"id": 5}], "next_page": None},
    ]
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        body = pages[call_count["n"]]
        call_count["n"] += 1
        return httpx.Response(200, json=body)

    client = _make_client(httpx.MockTransport(handler))
    records = list(client.paginate("/api/v2/users.json"))
    assert [r["id"] for r in records] == [1, 2, 3, 4, 5]
    assert call_count["n"] == 3


def test_get_incremental_sets_end_of_stream() -> None:
    body = {
        "tickets": [{"id": 1}, {"id": 2}],
        "after_cursor": "abc123",
        "end_of_stream": True,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    client = _make_client(httpx.MockTransport(handler))
    page = client.get_incremental_cursor("/api/v2/incremental/tickets/cursor.json")
    assert page.end_of_stream is True
    assert page.next_cursor == "abc123"
    assert len(page.records) == 2


def test_retries_on_429() -> None:
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, text="Too Many")
        return httpx.Response(200, json={"users": [{"id": 1}], "next_page": None})

    client = _make_client(httpx.MockTransport(handler))
    page = client.get_page("/api/v2/users.json")
    assert call_count["n"] == 2
    assert len(page.records) == 1


def test_raises_zendesk_api_error_on_404() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="Not Found")

    call_count = {"n": 0}

    def counting_handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return handler(request)

    client = _make_client(httpx.MockTransport(counting_handler))
    with pytest.raises(ZendeskAPIError) as exc_info:
        client.get_page("/api/v2/missing.json")
    assert exc_info.value.status_code == 404
    assert call_count["n"] == 1


def test_rate_limiter_called_before_each_request() -> None:
    pages = [
        {
            "users": [{"id": 1}],
            "next_page": "https://acme.zendesk.com/api/v2/users.json?page=2",
        },
        {"users": [{"id": 2}], "next_page": None},
    ]
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        body = pages[call_count["n"]]
        call_count["n"] += 1
        return httpx.Response(200, json=body)

    rate_limiter_calls = {"n": 0}

    class CountingLimiter:
        def acquire(self) -> None:
            rate_limiter_calls["n"] += 1

    client = ZendeskClient(
        subdomain="acme",
        email="user@example.com",
        api_token="token",
        rate_limiter=CountingLimiter(),  # type: ignore[arg-type]
    )
    client._client = httpx.Client(  # type: ignore[assignment]
        base_url="https://acme.zendesk.com",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(30.0),
    )

    list(client.paginate("/api/v2/users.json"))
    assert rate_limiter_calls["n"] == 2


def test_paginate_incremental_stops_on_end_of_stream() -> None:
    pages: list[dict[str, Any]] = [
        {
            "tickets": [{"id": 1}],
            "after_cursor": "cur1",
            "end_of_stream": False,
        },
        {
            "tickets": [{"id": 2}, {"id": 3}],
            "after_cursor": "cur2",
            "end_of_stream": True,
        },
    ]
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        body = pages[call_count["n"]]
        call_count["n"] += 1
        return httpx.Response(200, json=body)

    client = _make_client(httpx.MockTransport(handler))
    records = list(client.paginate_incremental("/api/v2/incremental/tickets/cursor.json"))
    assert [r["id"] for r in records] == [1, 2, 3]
    assert client.last_cursor == "cur2"
