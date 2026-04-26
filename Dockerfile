FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ENV DAGSTER_HOME=/app/.dagster \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl poppler-utils zstd \
    && arch="$(dpkg --print-architecture)" \
    && curl -fsSL "https://ollama.com/download/ollama-linux-${arch}.tar.zst" \
        | zstd -d \
        | tar -x -C /usr/local \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --group dev --no-install-project

COPY README.md ./
COPY src ./src
COPY docker/dagster.yaml "$DAGSTER_HOME/dagster.yaml"
COPY docker/entrypoint.sh /usr/local/bin/personal-data-warehouse-entrypoint
COPY docker/start-dagster.sh /usr/local/bin/personal-data-warehouse-start-dagster
RUN uv sync --frozen --group dev \
    && chmod +x /usr/local/bin/personal-data-warehouse-entrypoint \
    && chmod +x /usr/local/bin/personal-data-warehouse-start-dagster

EXPOSE 3000

ENTRYPOINT ["personal-data-warehouse-entrypoint"]
CMD ["personal-data-warehouse-start-dagster"]
