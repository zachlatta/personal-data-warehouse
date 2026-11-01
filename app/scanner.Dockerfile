# pdw ingest scanner as a Coolify service on mew-coolify (app/scanner-compose.yaml).
FROM golang:1.26.10-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# No version is stamped in, so the CLI never tries to update itself here.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pdw ./cmd/pdw-cli \
    && mkdir -p /out/state

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/pdw /pdw
COPY --from=build --chown=65532:65532 /out/state /var/lib/pdw-scanner
ENTRYPOINT ["/pdw", "ingest", "scanner", "--spool-dir", "/var/lib/pdw-scanner/spool", "--lock-file", "/var/lib/pdw-scanner/scanner.lock"]
