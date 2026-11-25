# The agent's warehouse access is the real pdw CLI, compiled from this repo so
# the binary in the image always matches the checked-out app. It is never
# self-updated at runtime (PDW_NO_AUTO_UPDATE=1): the container is read-only and
# a background GitHub fetch would be both useless and unwanted here.
#
# Plain Dockerfile syntax only: production builds this from the Dagster
# container, whose Debian docker.io 20.10 CLI runs the legacy builder (no
# BuildKit cache mounts).

# The third-party packages pdw imports. This stage reruns on every app/ edit
# (a second or two), but its output changes only when that list does.
FROM golang:1.26.11-bookworm AS pdw-cli-deps
ENV CGO_ENABLED=0
WORKDIR /src
COPY app/go.mod app/go.sum ./
RUN go mod download
COPY app/ ./
RUN go list -deps -f '{{if not .Standard}}{{if not .Module.Main}}{{.ImportPath}}{{end}}{{end}}' ./cmd/pdw-cli \
    | sort > /deps.txt

# Std and those packages are nearly all of a cold compile (21.7 s of it vs
# 1.6 s after a CLI edit, 2026-11-25), so they compile into a cached layer
# keyed on the list, and a CLI edit recompiles only the module's own packages.
FROM golang:1.26.11-bookworm AS pdw-cli
ENV CGO_ENABLED=0
WORKDIR /src
COPY app/go.mod app/go.sum ./
RUN go mod download
COPY --from=pdw-cli-deps /deps.txt /deps.txt
RUN go build $(cat /deps.txt)
COPY app/ ./
RUN go build -o /out/pdw ./cmd/pdw-cli

FROM node:22-bookworm-slim

ENV PYTHONUNBUFFERED=1

# Install Debian's security updates on every build: Coolify reuses a cached
# base image, and the PDW image ran an 8-month-old OpenSSL and gnutls on
# 2026-10-24 (sysadmin porygon/2026-10-24-security-followup.md).
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates git python3 bash jq ripgrep curl \
    && npm install -g @openai/codex @anthropic-ai/claude-code \
    && rm -rf /var/lib/apt/lists/*

COPY --from=pdw-cli /out/pdw /usr/local/bin/pdw

COPY docker/agent-entrypoint.sh /usr/local/bin/personal-data-warehouse-agent-entrypoint
RUN chmod +x /usr/local/bin/personal-data-warehouse-agent-entrypoint

ENTRYPOINT ["personal-data-warehouse-agent-entrypoint"]
