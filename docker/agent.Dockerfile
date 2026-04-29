FROM node:22-bookworm-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git python3 bash jq ripgrep \
    && npm install -g @openai/codex @anthropic-ai/claude-code \
    && rm -rf /var/lib/apt/lists/*

COPY docker/agent-entrypoint.sh /usr/local/bin/personal-data-warehouse-agent-entrypoint
RUN chmod +x /usr/local/bin/personal-data-warehouse-agent-entrypoint

ENTRYPOINT ["personal-data-warehouse-agent-entrypoint"]
