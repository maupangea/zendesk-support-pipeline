from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa

from zendesk_ingestion.streams.tickets import (
    Ticket,
    TicketCommentAttachment,
    TicketTag,
)
from zendesk_ingestion.transform.fivetran import FivetranTransformer

_FIXTURE = Path(__file__).parents[2] / "fixtures" / "zendesk" / "tickets.json"


def _load_tickets() -> list[dict[str, Any]]:
    with _FIXTURE.open() as fh:
        return json.load(fh)["tickets"]


class FakeClient:
    """Minimal stand-in for ZendeskClient that replays fixture records."""

    def __init__(self, records: list[dict[str, Any]]) -> None:
        self._records = records
        self.last_cursor: str | None = None

    def paginate_incremental(
        self, path: str, cursor: str | None = None
    ) -> Iterator[dict[str, Any]]:
        self.last_cursor = "after"
        yield from self._records


def test_ticket_get_records_applies_transformer() -> None:
    client = FakeClient(_load_tickets())
    stream = Ticket(FivetranTransformer("ticket", deleted_field="status"))
    rows = list(stream.get_records(client, None))  # type: ignore[arg-type]
    assert rows
    assert all("_fivetran_synced" in row for row in rows)


def test_ticket_tag_derives_from_parent_records() -> None:
    parent_records = [
        {"id": 1, "tags": ["a", "b", "c"]},
        {"id": 2, "tags": ["x", "y", "z"]},
    ]
    stream = TicketTag(FivetranTransformer("ticket_tag"))
    rows = list(stream.get_records(FakeClient([]), None, parent_records))  # type: ignore[arg-type]
    assert len(rows) == 6


def test_ticket_tag_synthetic_id_is_stable() -> None:
    parent_records = [{"id": 7, "tags": ["urgent", "vip"]}]
    stream = TicketTag(FivetranTransformer("ticket_tag"))
    first = list(stream.get_records(FakeClient([]), None, parent_records))  # type: ignore[arg-type]
    second = list(stream.get_records(FakeClient([]), None, parent_records))  # type: ignore[arg-type]
    first_ids = {(r["ticket_id"], r["tag"]): r["_fivetran_id"] for r in first}
    second_ids = {(r["ticket_id"], r["tag"]): r["_fivetran_id"] for r in second}
    assert first_ids == second_ids


def test_ticket_marks_deleted_tickets() -> None:
    stream = Ticket(FivetranTransformer("ticket", deleted_field="status"))
    rows = {
        row["id"]: row
        for row in stream.get_records(FakeClient(_load_tickets()), None)  # type: ignore[arg-type]
    }
    assert rows[35004]["_fivetran_deleted"] is True
    assert rows[35001]["_fivetran_deleted"] is False


def test_ticket_schema_matches_parquet_output() -> None:
    tickets = _load_tickets()
    stream = Ticket(FivetranTransformer("ticket", deleted_field="status"))
    records = list(stream.get_records(FakeClient(tickets), None))  # type: ignore[arg-type]
    table = pa.Table.from_pylist(records, schema=stream.get_schema())
    assert table.num_rows == len(tickets)
    assert TicketCommentAttachment.name == "ticket_comment_attachment"
