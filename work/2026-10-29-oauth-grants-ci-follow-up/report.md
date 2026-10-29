# OAuth MCP connector grants and CI follow-up (`0ff92eb`)

Checked 2026-10-29 at 04:05 UTC (~13.3 hours after `0ff92eb` deployed at 2026-10-28 14:46:56 UTC). Read-only production and GitHub Actions checks; no service or code changes.

## 1. OAuth connector grants in `private.app_access_tokens` and `/mcp` traffic

Both active Claude connectors refreshed their pre-grant OAuth tokens via `POST /oauth/token` (`200 OK`) and created `kind = 'oauth'`, `scope = 'query'` grant rows in `private.app_access_tokens` with the expected 90-day (`7,776,000 s`) idle timeout and unrevoked sentinel timestamps (`1970-01-01 00:00:00+00`):

| Client label | Kind | Scope | Created at (UTC) | Last used at (UTC) | Idle timeout (s) | Revoked at |
| --- | --- | --- | --- | --- | ---: | --- |
| `Claude Desktop` | `oauth` | `query` | `2026-10-28 22:54:48.698434+00` | `2026-10-28 23:21:41.825436+00` | `7776000` | `1970-01-01 00:00:00+00` |
| `claude (account)` | `oauth` | `query` | `2026-10-28 23:18:39.284678+00` | `2026-10-29 04:05:02.761947+00` | `7776000` | `1970-01-01 00:00:00+00` |

Across the entire post-deploy window (`2026-10-28T14:47:04.954Z` through `2026-10-29T04:05:05.859Z`), application logs record **1,019** `POST /mcp` requests and **2** `POST /oauth/token` requests with **zero `401`**, **zero `403`**, and **zero `5xx`** responses:

| Path | Method | Status | Client label | Requests |
| --- | --- | ---: | --- | ---: |
| `/mcp` | `POST` | `200` | `Claude Desktop` | `344` |
| `/mcp` | `POST` | `200` | `claude (account)` | `331` |
| `/mcp` | `POST` | `202` | `Claude Desktop` | `172` |
| `/mcp` | `POST` | `202` | `claude (account)` | `166` |
| `/mcp` | `POST` | `400` | `claude (account)` | `4` |
| `/mcp` | `POST` | `200` | `chatgpt` | `1` |
| `/mcp` | `POST` | `400` | `Claude Desktop` | `1` |
| `/oauth/token` | `POST` | `200` | *(token refresh)* | `2` |

The five `400` responses on `/mcp` (`bytes=108`) are normal MCP session-state reconnects where bearer authentication succeeded (the authenticated `client` attribute is populated on each log entry) before the streamable HTTP session re-initialized with `200 OK`.

## 2. GitHub Actions CI runs for `0ff92eb`

Both workflows for commit `0ff92ebbadc9c188cae1898fd7c543d758f40cc6` completed with `conclusion = success` across all nine check runs:

| Workflow | Workflow run ID | Check run ID | Job name | Status | Conclusion | Started at (UTC) | Completed at (UTC) |
| --- | ---: | ---: | --- | --- | --- | --- | --- |
| Python tests | `40709328939` | `122889974790` | `Lint` | `completed` | `success` | `2026-10-28T14:46:58Z` | `2026-10-28T14:47:06Z` |
| Python tests | `40709328939` | `122889974280` | `Contract suites` | `completed` | `success` | `2026-10-28T14:46:58Z` | `2026-10-28T14:52:44Z` |
| pdw-cli Build and Release | `40709328922` | `122889972600` | `Go tests` | `completed` | `success` | `2026-10-28T14:47:26Z` | `2026-10-28T14:48:25Z` |
| pdw-cli Build and Release | `40709328922` | `122890849778` | `Resolve version` | `completed` | `success` | `2026-10-28T14:48:57Z` | `2026-10-28T14:49:03Z` |
| pdw-cli Build and Release | `40709328922` | `122891221534` | `Build darwin/amd64` | `completed` | `success` | `2026-10-28T14:49:34Z` | `2026-10-28T14:50:28Z` |
| pdw-cli Build and Release | `40709328922` | `122891221566` | `Build darwin/arm64` | `completed` | `success` | `2026-10-28T14:50:59Z` | `2026-10-28T14:51:51Z` |
| pdw-cli Build and Release | `40709328922` | `122891222189` | `Build linux/arm64` | `completed` | `success` | `2026-10-28T14:52:22Z` | `2026-10-28T14:53:13Z` |
| pdw-cli Build and Release | `40709328922` | `122891221667` | `Build linux/amd64` | `completed` | `success` | `2026-10-28T14:53:45Z` | `2026-10-28T14:54:38Z` |
| pdw-cli Build and Release | `40709328922` | `122894521155` | `Publish release` | `completed` | `success` | `2026-10-28T14:56:42Z` | `2026-10-28T14:56:53Z` |

## Reproduction

`check.py` runs read-only queries against `private.app_access_tokens` (redacting account emails and omitting token identifiers and hashes), aggregates `/mcp` and `/oauth/token` HTTP status logs since the `0ff92eb` deploy, and fetches GitHub Actions check runs for `0ff92ebbadc9c188cae1898fd7c543d758f40cc6`. Set `AUDIT_HOST`, `WAREHOUSE_CONTAINER`, `LOKI_SCRIPT`, and `APP_LOKI_SELECTOR` to the current production placement.

Validation: `uv run pytest` completed with 3,251 passed and five skipped in 73.50 seconds. Staged content reviewed for public disclosure; only non-sensitive operational metadata, HTTP status counts, CI check-run outcomes, and reproduction queries are included. No application code changed.

