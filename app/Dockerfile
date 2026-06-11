FROM golang:1.26.2-bookworm AS build

WORKDIR /src
ARG PDW_GIT_SHA=""
ARG GIT_SHA=""
ARG SOURCE_COMMIT=""
ARG GIT_COMMIT=""
ARG COMMIT_SHA=""
ARG COOLIFY_GIT_COMMIT=""
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN git_sha="${PDW_GIT_SHA:-${SOURCE_COMMIT:-${GIT_SHA:-${GIT_COMMIT:-${COMMIT_SHA:-${COOLIFY_GIT_COMMIT:-unknown}}}}}}" \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X github.com/zachlatta/personal-data-warehouse/app/internal/buildinfo.gitSHA=${git_sha}" -o /out/pdw-mcp ./cmd/pdw-mcp

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/pdw-mcp /pdw-mcp
EXPOSE 8080
ENTRYPOINT ["/pdw-mcp"]
