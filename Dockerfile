FROM python:3.12-slim AS builder

RUN pip install --no-cache-dir uv
WORKDIR /app

# Source is required for `uv sync` to build and install the project itself
# (which provides the `zendesk-ingestion` console script).
COPY pyproject.toml uv.lock ./
COPY src/ src/
RUN uv sync --no-dev --frozen

FROM python:3.12-slim

WORKDIR /app
COPY --from=builder /app/.venv .venv
COPY src/ src/
COPY config/ config/

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["zendesk-ingestion"]
CMD ["sync"]
