FROM ghcr.io/astral-sh/uv:python3.10-bookworm-slim AS builder

WORKDIR /app
COPY pyproject.toml ./
RUN uv sync --frozen --no-install-project --no-editable
COPY *.py ./
RUN uv sync --frozen --no-editable

FROM python:3.10-slim

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/*.py /app/

ENV PATH="/app/.venv/bin:$PATH"

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]