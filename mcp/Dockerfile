FROM golang:1.26-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pdw-mcp ./cmd/pdw-mcp

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/pdw-mcp /pdw-mcp
EXPOSE 8080
ENTRYPOINT ["/pdw-mcp"]
