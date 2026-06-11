"""Stream package — importing it registers every stream in STREAM_REGISTRY."""

from __future__ import annotations

# Import all stream modules to trigger their @register decorators.
from zendesk_ingestion.streams import (
    automations,
    brands,
    groups,
    macros,
    organizations,
    satisfaction_ratings,
    schedules,
    sla_policies,
    tags,
    ticket_audits,
    ticket_fields,
    ticket_forms,
    ticket_metrics,
    tickets,
    triggers,
    users,
    views,
)
from zendesk_ingestion.streams.registry import STREAM_REGISTRY

__all__ = [
    "STREAM_REGISTRY",
    "automations",
    "brands",
    "groups",
    "macros",
    "organizations",
    "satisfaction_ratings",
    "schedules",
    "sla_policies",
    "tags",
    "ticket_audits",
    "ticket_fields",
    "ticket_forms",
    "ticket_metrics",
    "tickets",
    "triggers",
    "users",
    "views",
]
