from __future__ import annotations

from datetime import UTC, datetime

from zendesk_ingestion.transform.fivetran import (
    FIVETRAN_DELETED_COL,
    FIVETRAN_ID_COL,
    FIVETRAN_SYNCED_COL,
    FivetranTransformer,
)


def test_flatten_nested_via_object() -> None:
    t = FivetranTransformer(stream_name="ticket")
    out = t.transform_record({"id": 1, "via": {"channel": "web", "source": {"from": {}}}})
    assert out["via_channel"] == "web"
    assert "via" not in out


def test_flatten_does_not_flatten_lists() -> None:
    t = FivetranTransformer(stream_name="ticket")
    out = t.transform_record({"id": 1, "tags": ["a", "b", "c"]})
    assert out["tags"] == ["a", "b", "c"]


def test_cast_timestamps_converts_iso_strings() -> None:
    t = FivetranTransformer(stream_name="ticket")
    out = t.transform_record({"id": 1, "created_at": "2024-01-15T10:30:00Z"})
    assert isinstance(out["created_at"], datetime)
    assert out["created_at"].tzinfo is not None
    assert out["created_at"].year == 2024


def test_cast_timestamps_handles_offset_format() -> None:
    t = FivetranTransformer(stream_name="ticket")
    out = t.transform_record({"id": 1, "created_at": "2024-01-15T10:30:00+02:00"})
    assert isinstance(out["created_at"], datetime)
    assert out["created_at"].utcoffset() is not None


def test_fivetran_synced_is_utc_datetime() -> None:
    t = FivetranTransformer(stream_name="ticket")
    out = t.transform_record({"id": 1})
    assert isinstance(out[FIVETRAN_SYNCED_COL], datetime)
    assert out[FIVETRAN_SYNCED_COL].tzinfo == UTC


def test_is_deleted_true_when_status_deleted() -> None:
    t = FivetranTransformer(stream_name="ticket", deleted_field="status")
    out = t.transform_record({"id": 1, "status": "deleted"})
    assert out[FIVETRAN_DELETED_COL] is True


def test_is_deleted_false_when_status_open() -> None:
    t = FivetranTransformer(stream_name="ticket", deleted_field="status")
    out = t.transform_record({"id": 1, "status": "open"})
    assert out[FIVETRAN_DELETED_COL] is False


def test_is_deleted_false_when_no_deleted_field_configured() -> None:
    t = FivetranTransformer(stream_name="ticket")
    out = t.transform_record({"id": 1, "status": "deleted"})
    assert out[FIVETRAN_DELETED_COL] is False


def test_synthetic_id_is_stable() -> None:
    t = FivetranTransformer(stream_name="ticket_tag")
    r1 = t.add_synthetic_id({"ticket_id": 42, "tag": "urgent"}, "ticket_id", "tag")
    r2 = t.add_synthetic_id({"ticket_id": 42, "tag": "urgent"}, "ticket_id", "tag")
    assert r1[FIVETRAN_ID_COL] == r2[FIVETRAN_ID_COL]


def test_synthetic_id_differs_for_different_inputs() -> None:
    t = FivetranTransformer(stream_name="ticket_tag")
    r1 = t.add_synthetic_id({"ticket_id": 42, "tag": "urgent"}, "ticket_id", "tag")
    r2 = t.add_synthetic_id({"ticket_id": 42, "tag": "low"}, "ticket_id", "tag")
    assert r1[FIVETRAN_ID_COL] != r2[FIVETRAN_ID_COL]
