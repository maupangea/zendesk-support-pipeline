# Phase 2 — Stream Implementations

Implement all Zendesk streams. Each stream is a class in `src/zendesk_ingestion/streams/`. Phase 1 must be complete before starting this phase.

---

## Task 2.1 — AbstractStream base class

**`src/zendesk_ingestion/streams/base.py`**:

```python
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Iterator
import pyarrow as pa
import structlog

from zendesk_ingestion.api.client import ZendeskClient
from zendesk_ingestion.config.models import SyncMode
from zendesk_ingestion.transform.fivetran import FivetranTransformer


class AbstractStream(ABC):
    # Override in each subclass — used as S3 path component and DynamoDB sort key
    name: str

    # Zendesk API path for this stream (may be None for derived streams)
    endpoint: str | None = None

    # Field used as cursor for incremental syncs (None = no incremental support)
    cursor_field: str | None = None

    # Fields that uniquely identify a record (used for deduplication key)
    primary_key: list[str] = ["id"]

    # Default sync mode — overridden per-stream in connector.yaml
    default_sync_mode: SyncMode = SyncMode.INCREMENTAL_DEDUPED

    # Whether this stream is derived from a parent stream's response
    # If set, the orchestrator will skip the direct API call and pass
    # parent_records instead.
    parent_stream: str | None = None

    def __init__(self, transformer: FivetranTransformer) -> None:
        self._transformer = transformer
        self._log = structlog.get_logger().bind(stream=self.name)

    @abstractmethod
    def get_schema(self) -> pa.Schema:
        """Return the pyarrow schema for this stream's output Parquet files."""

    @abstractmethod
    def get_records(
        self,
        client: ZendeskClient,
        cursor: str | None,
        parent_records: list[dict[str, Any]] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        Yield transformed records ready for Parquet serialization.
        For derived streams, `parent_records` contains the parent's raw API records.
        For API streams, `client` and `cursor` are used directly.
        Must apply self._transformer.transform_record() to each record before yielding.
        """

    def get_final_cursor(self, client: ZendeskClient) -> str | None:
        """
        Return the cursor value to persist after a successful sync.
        Default: return client.last_cursor (set by paginate_incremental).
        Override for streams that derive cursor differently.
        """
        return client.last_cursor
```

---

## Task 2.2 — Stream registry

**`src/zendesk_ingestion/streams/registry.py`**:

```python
from zendesk_ingestion.streams.base import AbstractStream

# Populated after all stream modules are imported.
# Maps stream name (str) → stream class (type[AbstractStream])
STREAM_REGISTRY: dict[str, type[AbstractStream]] = {}


def register(cls: type[AbstractStream]) -> type[AbstractStream]:
    """Class decorator that registers a stream class in STREAM_REGISTRY."""
    STREAM_REGISTRY[cls.name] = cls
    return cls
```

Every stream class must be decorated with `@register`.

---

## Task 2.3 — Implement ticket streams

**`src/zendesk_ingestion/streams/tickets.py`** — implement four classes:

### `Ticket`
- `name = "ticket"`
- `endpoint = "/api/v2/incremental/tickets/cursor.json"`
- `cursor_field = "updated_at"`
- `default_sync_mode = SyncMode.INCREMENTAL_DEDUPED`
- `get_records`: call `client.paginate_incremental(self.endpoint, cursor)`, apply transformer
- Transformer: flatten `via` object, cast timestamps, mark `_fivetran_deleted` when `status == "deleted"`
- Schema: include all fields from the Fivetran ticket table (see plan Section 10). Use `pa.timestamp("us", tz="UTC")` for all timestamp columns.

### `TicketTag`  
- `name = "ticket_tag"`
- `parent_stream = "ticket"`
- `default_sync_mode = SyncMode.INCREMENTAL_DEDUPED`
- `primary_key = ["_fivetran_id"]`
- `get_records`: iterate `parent_records`, for each ticket yield one record per tag in `ticket["tags"]`. Call `transformer.add_synthetic_id(record, "ticket_id", "tag")`.
- Schema: `ticket_id` (int64), `tag` (string), `_fivetran_id` (string), `_fivetran_synced` (timestamp), `_fivetran_deleted` (bool)

### `TicketComment`
- `name = "ticket_comment"`
- `endpoint = "/api/v2/incremental/ticket_events.json"`
- `cursor_field = "created_at"`
- `default_sync_mode = SyncMode.INCREMENTAL_APPEND`
- `get_records`: filter events where `event_type == "Comment"`, yield comment records

### `TicketCommentAttachment`
- `name = "ticket_comment_attachment"`
- `parent_stream = "ticket_comment"`
- `default_sync_mode = SyncMode.INCREMENTAL_APPEND`
- `get_records`: flatten `attachments` array from each comment record

Write fixture file `tests/fixtures/zendesk/tickets.json` containing a realistic sample API response (3–5 tickets with varied statuses, tags, via objects). Use fake but realistic data.

Write tests in `tests/unit/streams/test_tickets.py`:
- `test_ticket_get_records_applies_transformer` — mock client, assert `_fivetran_synced` present
- `test_ticket_tag_derives_from_parent_records` — 2 tickets × 3 tags = 6 records
- `test_ticket_tag_synthetic_id_is_stable`
- `test_ticket_marks_deleted_tickets`
- `test_ticket_schema_matches_parquet_output` — write records with the schema, assert no type errors

---

## Task 2.4 — Implement user streams

**`src/zendesk_ingestion/streams/users.py`** — implement:

### `User`
- `endpoint = "/api/v2/incremental/users/cursor.json"`
- `cursor_field = "updated_at"`
- `default_sync_mode = SyncMode.INCREMENTAL_DEDUPED`
- Flatten `user_fields` custom field values as `user_field_{key}` columns

### `UserTag`
- `parent_stream = "user"`
- Same pattern as `TicketTag` but sourced from `user["tags"]`

### `UserField`
- `endpoint = "/api/v2/user_fields.json"`
- `default_sync_mode = SyncMode.INCREMENTAL_DEDUPED`

### `UserFieldOption`
- `endpoint = "/api/v2/user_fields/{id}/options.json"` (requires iterating user field IDs)
- `default_sync_mode = SyncMode.FULL_REFRESH_OVERWRITE`
- `get_records`: first fetch all user fields, then for each field fetch its options

### `UserIdentity`
- `endpoint = "/api/v2/users/{id}/identities.json"`
- `default_sync_mode = SyncMode.INCREMENTAL_DEDUPED`
- `get_records`: accept list of user_ids from parent records, paginate each user's identities

---

## Task 2.5 — Implement organization streams

**`src/zendesk_ingestion/streams/organizations.py`** — implement:

- `Organization` — `/api/v2/incremental/organizations/cursor.json`, INCREMENTAL_DEDUPED
- `OrganizationTag` — parent_stream="organization", derived from `org["tags"]`
- `OrganizationField` — `/api/v2/organization_fields.json`, INCREMENTAL_DEDUPED
- `OrganizationFieldOption` — per-field options, FULL_REFRESH_OVERWRITE
- `OrganizationMember` — `/api/v2/organizations/{id}/users.json`, FULL_REFRESH_OVERWRITE

Same patterns as user streams.

---

## Task 2.6 — Implement group streams

**`src/zendesk_ingestion/streams/groups.py`**:

- `Group` — `/api/v2/groups.json`, INCREMENTAL_DEDUPED
- `GroupMembership` — `/api/v2/group_memberships.json`, INCREMENTAL_DEDUPED

---

## Task 2.7 — Implement ticket metric streams

**`src/zendesk_ingestion/streams/ticket_metrics.py`**:

### `TicketMetricEvent`
- `endpoint = "/api/v2/incremental/ticket_metric_events.json"`
- `cursor_field = "time"`
- `default_sync_mode = SyncMode.INCREMENTAL_APPEND`
- Note: cursor for this endpoint is a Unix timestamp integer, not ISO string. Store as string `"1234567890"`, cast to int before sending to API.

### `TicketMetric`
- Derived from `TicketMetricEvent` — aggregate metric values per ticket

---

## Task 2.8 — Implement ticket audit and field streams

**`src/zendesk_ingestion/streams/ticket_audits.py`**:
- `TicketAudit` — `/api/v2/incremental/ticket_events.json`, INCREMENTAL_APPEND, filter for audit events
- `TicketEmailCc` — derived from TicketAudit events of type `cc`
- `TicketFollower` — derived from TicketAudit events of type `follower`

**`src/zendesk_ingestion/streams/ticket_fields.py`**:
- `TicketField` — `/api/v2/ticket_fields.json`, INCREMENTAL_DEDUPED
- `TicketFieldOption` — per-field options, FULL_REFRESH_OVERWRITE

**`src/zendesk_ingestion/streams/ticket_forms.py`**:
- `TicketForm` — `/api/v2/ticket_forms.json`, INCREMENTAL_DEDUPED
- `TicketFormCondition` — derived from ticket form conditions arrays

---

## Task 2.9 — Implement configuration streams

These are all smaller, lower-volume streams. Implement in a single pass.

**`src/zendesk_ingestion/streams/satisfaction_ratings.py`**:
- `SatisfactionRating` — `/api/v2/satisfaction_ratings.json?start_time={cursor}`, INCREMENTAL_DEDUPED

**`src/zendesk_ingestion/streams/brands.py`**:
- `Brand` — `/api/v2/brands.json`, INCREMENTAL_DEDUPED

**`src/zendesk_ingestion/streams/macros.py`**:
- `Macro` — `/api/v2/macros.json`, INCREMENTAL_DEDUPED
- `MacroAttachment` — `/api/v2/macros/{id}/attachments.json`, FULL_REFRESH_OVERWRITE

**`src/zendesk_ingestion/streams/views.py`**:
- `View` — `/api/v2/views.json`, INCREMENTAL_DEDUPED

**`src/zendesk_ingestion/streams/automations.py`**:
- `Automation` — `/api/v2/automations.json`, INCREMENTAL_DEDUPED
- `AutomationAction` — derived from `automation["actions"]`, FULL_REFRESH_OVERWRITE
- `AutomationCondition` — derived from `automation["conditions"]["all"] + ["any"]`, FULL_REFRESH_OVERWRITE

**`src/zendesk_ingestion/streams/triggers.py`**:
- `Trigger` — `/api/v2/triggers.json`, INCREMENTAL_DEDUPED
- `TriggerAction` — derived from `trigger["actions"]`
- `TriggerCondition` — derived from `trigger["conditions"]["all"] + ["any"]`

**`src/zendesk_ingestion/streams/sla_policies.py`**:
- `SlaPolicy` — `/api/v2/slas/policies.json`, FULL_REFRESH_OVERWRITE
- `SlaPolicyFilter` — derived from `sla["filter"]["all"] + ["any"]`
- `SlaPolicyCondition` — derived from `sla["policy_metrics"]`

**`src/zendesk_ingestion/streams/schedules.py`**:
- `Schedule` — `/api/v2/business_hours/schedules.json`, FULL_REFRESH_OVERWRITE
- `ScheduleHoliday` — `/api/v2/business_hours/schedules/{id}/holidays.json`, FULL_REFRESH_OVERWRITE

**`src/zendesk_ingestion/streams/tags.py`**:
- `Tag` — `/api/v2/tags.json`, FULL_REFRESH_OVERWRITE

---

## Task 2.10 — Populate stream registry

**Update `src/zendesk_ingestion/streams/__init__.py`** to import all stream modules (which triggers the `@register` decorator for each class):

```python
# Import all stream modules to trigger @register decorators
from zendesk_ingestion.streams import (
    tickets,
    ticket_metrics,
    ticket_audits,
    ticket_fields,
    ticket_forms,
    users,
    organizations,
    groups,
    satisfaction_ratings,
    brands,
    macros,
    views,
    automations,
    triggers,
    sla_policies,
    schedules,
    tags,
)

__all__ = ["STREAM_REGISTRY"]
from zendesk_ingestion.streams.registry import STREAM_REGISTRY
```

Verify the registry is complete:
```bash
uv run python -c "
from zendesk_ingestion.streams import STREAM_REGISTRY
print(f'{len(STREAM_REGISTRY)} streams registered:')
for name in sorted(STREAM_REGISTRY):
    print(f'  {name}')
"
```
Expected output: 40+ stream names.

---

## Verification checklist for Phase 2

```bash
uv run pytest tests/unit/streams/ -v        # all stream unit tests pass
uv run mypy src/zendesk_ingestion/streams/  # no type errors in streams
uv run ruff check src/zendesk_ingestion/streams/

# Spot-check schema round-trip for ticket stream
uv run python -c "
from zendesk_ingestion.streams.registry import STREAM_REGISTRY
from zendesk_ingestion.transform.fivetran import FivetranTransformer
import json, pathlib

fixture = json.loads(pathlib.Path('tests/fixtures/zendesk/tickets.json').read_text())
stream_cls = STREAM_REGISTRY['ticket']
transformer = FivetranTransformer('ticket', deleted_field='status')
stream = stream_cls(transformer)
schema = stream.get_schema()
print('Ticket schema fields:', [f.name for f in schema])
"
```
