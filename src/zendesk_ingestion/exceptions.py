class ZendeskIngestionError(Exception):
    """Base exception for all ingestion errors."""


class ZendeskRateLimitError(ZendeskIngestionError):
    """Raised when Zendesk returns 429. Includes retry_after seconds."""

    def __init__(self, retry_after: int = 60) -> None:
        self.retry_after = retry_after
        super().__init__(f"Rate limited. Retry after {retry_after}s.")


class ZendeskAPIError(ZendeskIngestionError):
    """Raised on non-retryable Zendesk API errors (4xx except 429)."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"Zendesk API error {status_code}: {message}")


class StateConflictError(ZendeskIngestionError):
    """Raised when a DynamoDB conditional write fails (stale cursor)."""


class S3WriteError(ZendeskIngestionError):
    """Raised on unrecoverable S3 write failure."""
