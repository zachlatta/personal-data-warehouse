# pdw mutations printer as a Coolify service on mew-coolify, beside the
# scanner (app/scanner-compose.yaml). The Brother takes PWG raster, not PDF:
# poppler-utils counts a PDF's pages, Ghostscript renders them and ipptool
# (cups-ipp-utils) sends the job. No CUPS daemon, so no spool that could
# hold or resend a job behind the worker's back.
FROM golang:1.26.10-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# No version is stamped in, so the CLI never tries to update itself here.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pdw ./cmd/pdw-cli

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ghostscript cups-ipp-utils poppler-utils ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 65532 --home-dir /nonexistent --shell /usr/sbin/nologin pdw
COPY --from=build /out/pdw /usr/local/bin/pdw
USER 65532
ENTRYPOINT ["/usr/local/bin/pdw", "mutations", "printer"]
