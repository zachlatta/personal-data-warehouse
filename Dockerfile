FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ENV DAGSTER_HOME=/app/.dagster \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --group dev --no-install-project

COPY README.md ./
COPY src ./src
COPY docker/dagster.yaml "$DAGSTER_HOME/dagster.yaml"
COPY docker/entrypoint.sh /usr/local/bin/personal-data-warehouse-entrypoint
RUN uv sync --frozen --group dev \
    && chmod +x /usr/local/bin/personal-data-warehouse-entrypoint

EXPOSE 3000

ENTRYPOINT ["personal-data-warehouse-entrypoint"]
CMD ["uv", "run", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "personal_data_warehouse.definitions"]
