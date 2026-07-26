# The agent's warehouse access is the real pdw CLI, compiled from this repo so
# the binary in the image always matches the checked-out app. It is never
# self-updated at runtime (PDW_NO_AUTO_UPDATE=1): the container is read-only and
# a background GitHub fetch would be both useless and unwanted here.
FROM golang:1.26-bookworm AS pdw-cli

WORKDIR /src
COPY app/go.mod app/go.sum ./
RUN go mod download
COPY app/ ./
RUN CGO_ENABLED=0 go build -o /out/pdw ./cmd/pdw-cli

FROM node:22-bookworm-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git python3 bash jq ripgrep curl \
    && npm install -g @openai/codex @anthropic-ai/claude-code \
    && rm -rf /var/lib/apt/lists/*

COPY --from=pdw-cli /out/pdw /usr/local/bin/pdw

COPY docker/agent-entrypoint.sh /usr/local/bin/personal-data-warehouse-agent-entrypoint
RUN chmod +x /usr/local/bin/personal-data-warehouse-agent-entrypoint

ENTRYPOINT ["personal-data-warehouse-agent-entrypoint"]
