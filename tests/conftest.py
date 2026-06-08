"""Shared pytest fixtures: mock S3, mock DynamoDB, sample records."""

from __future__ import annotations

from collections.abc import Iterator

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_dynamodb.service_resource import Table


@pytest.fixture
def aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def mock_dynamo(aws_credentials: None) -> Iterator[Table]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName="zendesk_ingestion_state",
            KeySchema=[
                {"AttributeName": "connector_id", "KeyType": "HASH"},
                {"AttributeName": "stream_name", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "connector_id", "AttributeType": "S"},
                {"AttributeName": "stream_name", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        client.get_waiter("table_exists").wait(TableName="zendesk_ingestion_state")
        table = boto3.resource("dynamodb", region_name="us-east-1").Table("zendesk_ingestion_state")
        yield table


@pytest.fixture
def mock_s3(aws_credentials: None) -> Iterator[str]:
    with mock_aws():
        bucket = "test-bucket"
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=bucket)
        yield bucket
