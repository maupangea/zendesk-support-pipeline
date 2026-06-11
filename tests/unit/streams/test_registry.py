"""Integration smoke tests over the whole stream registry."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
import yaml

from zendesk_ingestion.streams import STREAM_REGISTRY
from zendesk_ingestion.streams.base import AbstractStream
from zendesk_ingestion.transform.fivetran import (
    FIVETRAN_DELETED_COL,
    FIVETRAN_ID_COL,
    FIVETRAN_SYNCED_COL,
    FivetranTransformer,
)

_CONNECTOR_YAML = Path(__file__).parents[3] / "config" / "connector.yaml"


def test_registry_has_at_least_40_streams() -> None:
    assert len(STREAM_REGISTRY) >= 40


def test_registry_keys_match_class_name_attribute() -> None:
    for name, cls in STREAM_REGISTRY.items():
        assert cls.name == name


def test_registry_matches_connector_yaml_streams() -> None:
    config = yaml.safe_load(_CONNECTOR_YAML.read_text())
    yaml_streams = set(config["streams"])
    assert set(STREAM_REGISTRY) == yaml_streams


@pytest.mark.parametrize("name", sorted(STREAM_REGISTRY))
def test_stream_instantiates_and_exposes_valid_schema(name: str) -> None:
    cls = STREAM_REGISTRY[name]
    stream = cls(FivetranTransformer(stream_name=name))
    assert isinstance(stream, AbstractStream)

    schema = stream.get_schema()
    assert isinstance(schema, pa.Schema)

    field_names = set(schema.names)
    assert FIVETRAN_SYNCED_COL in field_names
    assert FIVETRAN_DELETED_COL in field_names

    # An empty round-trip proves the schema is constructible by the writer.
    table = pa.Table.from_pylist([], schema=schema)
    assert table.num_rows == 0


@pytest.mark.parametrize("name", sorted(STREAM_REGISTRY))
def test_synthetic_id_streams_declare_fivetran_id_column(name: str) -> None:
    cls = STREAM_REGISTRY[name]
    if cls.primary_key != [FIVETRAN_ID_COL]:
        pytest.skip("stream does not key on _fivetran_id")
    schema = cls(FivetranTransformer(stream_name=name)).get_schema()
    assert FIVETRAN_ID_COL in set(schema.names)
