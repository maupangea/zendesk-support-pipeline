"""Load ConnectorConfig from a YAML file, resolving secrets from AWS SSM / Secrets Manager."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import boto3
import yaml

from zendesk_ingestion.config.models import ConnectorConfig


def load_config(yaml_path: Path, *, resolve_secrets: bool = True) -> ConnectorConfig:
    """
    Parse connector.yaml and optionally resolve ARN references in the zendesk block.
    ARN values (starting with 'arn:aws:secretsmanager' or 'arn:aws:ssm') are fetched
    from AWS and their values substituted in-place before validation.
    """
    raw: dict[str, Any] = yaml.safe_load(yaml_path.read_text())

    if resolve_secrets:
        raw = _resolve_aws_refs(raw)

    return ConnectorConfig.model_validate(raw)


def _resolve_aws_refs(config: dict[str, Any]) -> dict[str, Any]:
    """Walk the config dict and replace ARN string values with their resolved secrets."""
    sm_client = boto3.client("secretsmanager")
    ssm_client = boto3.client("ssm")

    def resolve(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        if value.startswith("arn:aws:secretsmanager"):
            sm_resp = sm_client.get_secret_value(SecretId=value)
            secret = sm_resp.get("SecretString", "")
            try:
                return json.loads(secret)
            except json.JSONDecodeError:
                return secret
        if value.startswith("arn:aws:ssm") or value.startswith("/"):
            ssm_resp = ssm_client.get_parameter(Name=value, WithDecryption=True)
            return ssm_resp["Parameter"]["Value"]
        return value

    def walk(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: walk(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [walk(i) for i in obj]
        return resolve(obj)

    result: dict[str, Any] = walk(config)
    return result
