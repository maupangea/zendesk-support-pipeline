from __future__ import annotations

from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from zendesk_ingestion.config.loader import load_config


@pytest.fixture
def _aws(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def _yaml_file(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "connector.yaml"
    p.write_text(body)
    return p


def test_load_config_without_secret_resolution(tmp_path: Path) -> None:
    body = """
connector_id: zendesk_support
s3:
  bucket: bucket
  prefix: prefix
state:
  dynamodb_table: state_table
zendesk_subdomain: literal_subdomain
zendesk_email: literal_email
zendesk_api_token: literal_token
streams:
  ticket:
    sync_mode: incremental_append
"""
    cfg = load_config(_yaml_file(tmp_path, body), resolve_secrets=False)
    assert cfg.connector_id == "zendesk_support"
    assert cfg.s3.bucket == "bucket"
    assert cfg.zendesk_subdomain == "literal_subdomain"
    assert "ticket" in cfg.streams


def test_load_config_resolves_ssm_parameter(tmp_path: Path, _aws: None) -> None:
    with mock_aws():
        ssm = boto3.client("ssm", region_name="us-east-1")
        ssm.put_parameter(
            Name="/zendesk/subdomain",
            Value="acme",
            Type="String",
        )
        body = """
connector_id: zd
s3:
  bucket: b
  prefix: p
state:
  dynamodb_table: t
zendesk_subdomain: /zendesk/subdomain
zendesk_email: literal_email
zendesk_api_token: literal_token
"""
        cfg = load_config(_yaml_file(tmp_path, body), resolve_secrets=True)
        assert cfg.zendesk_subdomain == "acme"


def test_load_config_resolves_secrets_manager(tmp_path: Path, _aws: None) -> None:
    with mock_aws():
        sm = boto3.client("secretsmanager", region_name="us-east-1")
        secret = sm.create_secret(Name="zendesk/api_token", SecretString="tok-abc")
        arn = secret["ARN"]

        body = f"""
connector_id: zd
s3:
  bucket: b
  prefix: p
state:
  dynamodb_table: t
zendesk_subdomain: literal_subdomain
zendesk_email: literal_email
zendesk_api_token: {arn}
"""
        cfg = load_config(_yaml_file(tmp_path, body), resolve_secrets=True)
        assert cfg.zendesk_api_token == "tok-abc"
