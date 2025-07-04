FROM python:3.10-slim as builder
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

ADD ./src /app

RUN uv sync --frozen --no-install-project --no-editable

RUN uv sync --frozen --no-editable

COPY --from=builder /app/modules /app/modules

ENTRYPOINT [ "python", "./entrypoint.py" ]