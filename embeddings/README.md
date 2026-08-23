# PDW embeddings server

This directory is the Coolify deployment definition for PDW's private,
OpenAI-compatible embeddings endpoint. It serves `Qwen/Qwen3-Embedding-4B` with
Hugging Face Text Embeddings Inference (TEI) on the NVIDIA GPU in `mew`.

Coolify should deploy the application to the physical `mew` server, rather than
to the `mew-coolify` VM. This keeps the GPU attached to its current host and
avoids VM downtime, PCI passthrough, a second NVIDIA driver installation, and a
change to the endpoint's tailnet address. The deployment is still created,
updated, monitored, and restarted by the Coolify control plane.

## Coolify application settings

- Repository: `zachlatta/personal-data-warehouse`
- Branch: `main`
- Build pack: Docker Compose
- Compose file: `/embeddings/docker-compose.yaml`
- Target server: `mew`
- Required environment variable: `EMBEDDINGS_BIND_ADDRESS`, set to `mew`'s
  tailnet IPv4 address
- Optional environment variables:
  - `EMBEDDINGS_PORT` (default `8485`)
  - `EMBEDDINGS_CACHE_PATH` (default `/opt/pdw-embeddings/hf-cache`)

The bind address is deliberately required. Binding to all interfaces would
unnecessarily expose an unauthenticated model endpoint on the LAN.

## Load-bearing runtime settings

- `--auto-truncate` is required because the model's context exceeds TEI's
  default batch token limit.
- `--max-client-batch-size 256` is required because the PDW indexer submits
  batches of 128 texts and TEI otherwise defaults to 32.
- The Hugging Face cache is persistent so a restart or redeploy does not
  download the 4B model again.
- The image is pinned by tag and digest. Update both intentionally and verify a
  real embeddings request before retiring the previous deployment.

The health endpoint is `GET /health`; the OpenAI-compatible API root consumed
by PDW is `http://<mew-tailnet-ip>:8485/v1` with the defaults above.
