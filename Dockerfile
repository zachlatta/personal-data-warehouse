FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ARG PDW_GIT_SHA=""
ARG GIT_SHA=""
ARG SOURCE_COMMIT=""
ARG GIT_COMMIT=""
ARG COMMIT_SHA=""
ARG COOLIFY_GIT_COMMIT=""

ENV DAGSTER_HOME=/app/.dagster \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl docker.io poppler-utils tesseract-ocr zstd \
    && arch="$(dpkg --print-architecture)" \
    && curl -fsSL "https://ollama.com/download/ollama-linux-${arch}.tar.zst" \
        | zstd -d \
        | tar -x -C /usr/local \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --group dev --no-install-project

COPY README.md ./
COPY workspace.yaml ./
COPY src ./src
COPY docker/agent.Dockerfile docker/agent.Dockerfile
COPY docker/agent-entrypoint.sh docker/agent-entrypoint.sh
COPY docker/dagster.yaml "$DAGSTER_HOME/dagster.yaml"
COPY docker/entrypoint.sh /usr/local/bin/personal-data-warehouse-entrypoint
COPY docker/start-dagster.sh /usr/local/bin/personal-data-warehouse-start-dagster
RUN git_sha="${PDW_GIT_SHA:-${SOURCE_COMMIT:-${GIT_SHA:-${GIT_COMMIT:-${COMMIT_SHA:-${COOLIFY_GIT_COMMIT:-unknown}}}}}}" \
    && printf '{"git_sha":"%s"}\n' "$git_sha" > /app/build-info.json \
    && uv sync --frozen --group dev \
    && chmod +x /usr/local/bin/personal-data-warehouse-entrypoint \
    && chmod +x /usr/local/bin/personal-data-warehouse-start-dagster

EXPOSE 3000

ENTRYPOINT ["personal-data-warehouse-entrypoint"]
CMD ["personal-data-warehouse-start-dagster"]
