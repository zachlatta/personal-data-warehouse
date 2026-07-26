"""Per-run warehouse proxy that fronts the app's `/api/tools` surface.

Agent containers get the real `pdw` CLI, but never the app's real bearer
token: the runner starts one of these proxies per run, points the container's
``PDW_API_URL`` at it and its ``PDW_SECRET_TOKEN`` at a random per-run token,
and the proxy forwards allowlisted calls upstream with the credential the
Dagster process holds. The proxy dies with the run, so a leaked container
token is worthless afterwards; while it lives it can only reach read-only
tools and stored objects.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import secrets
import threading
from typing import Any
from urllib import error, parse, request

# Tools an agent may reach. The app's CLI surface also carries
# propose_mutation / propose_mutation_help / _debug_cache_status; agent runs are
# read-only research, so those stay off. `get_rows` / `get_field` / `grep_rows`
# / `query` are MCP-only upstream and never appear here.
DEFAULT_AGENT_TOOL_ALLOWLIST = frozenset({"sql", "schema_overview", "describe_table", "get_object"})

DEFAULT_AGENT_TOOL_PROXY_MAX_ROWS = 50
DEFAULT_AGENT_TOOL_PROXY_CLIENT_NAME = "pdw-agent"
API_TOOLS_PATH = "/api/tools"
OBJECTS_PATH_PREFIX = "/objects/"
UPSTREAM_TIMEOUT_SECONDS = 300
OBJECT_STREAM_CHUNK_BYTES = 1 << 20
# Mirrors auth.MaxClientNameLen in the Go app.
MAX_CLIENT_NAME_CHARS = 64
# The app sits behind Cloudflare, which bans urllib's default User-Agent
# outright (error 1010, browser_signature_banned) — every upstream call 403s
# without this. Identify honestly; a descriptive agent string passes.
UPSTREAM_USER_AGENT = "pdw-agent-tool-proxy/1.0 (+personal-data-warehouse)"


@dataclass(frozen=True)
class WarehouseAppConfig:
    """The credential the Dagster process holds for the warehouse app."""

    base_url: str
    token: str
    client_name: str = DEFAULT_AGENT_TOOL_PROXY_CLIENT_NAME

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))

    @property
    def authorization(self) -> str:
        return f"Bearer {self.client_name}:{self.token}"


def warehouse_app_config_from_env(env: Mapping[str, str] | None = None) -> WarehouseAppConfig:
    """Resolve the app URL + token the same way every other client does.

    Fails loud rather than silently running agents without warehouse access:
    an enrichment agent that cannot query is worse than a run that stops.
    """

    env = os.environ if env is None else env
    base_url = (env.get("PDW_API_URL") or env.get("MCP_BASE_URL") or "").strip()
    token = (env.get("PDW_SECRET_TOKEN") or env.get("MCP_SECRET_TOKEN") or "").strip()
    missing = []
    if not base_url:
        missing.append("PDW_API_URL (or MCP_BASE_URL)")
    if not token:
        missing.append("PDW_SECRET_TOKEN (or MCP_SECRET_TOKEN)")
    if missing:
        raise RuntimeError(
            "agent runs need warehouse app access for the pdw CLI; set " + " and ".join(missing)
        )
    return WarehouseAppConfig(base_url=base_url, token=token)


def agent_client_name(base: str, run_id: str) -> str:
    """The client identifier this run reports to the app.

    The app rejects names over MAX_CLIENT_NAME_CHARS or containing ':', and a
    rejected name is a 401 — so the run id is trimmed to fit rather than
    allowed to fail the whole run.
    """

    base = (base or DEFAULT_AGENT_TOOL_PROXY_CLIENT_NAME).replace(":", "-").strip() or DEFAULT_AGENT_TOOL_PROXY_CLIENT_NAME
    run_id = run_id.replace(":", "-").strip()
    if not run_id:
        return base[:MAX_CLIENT_NAME_CHARS]
    return f"{base}-{run_id}"[:MAX_CLIENT_NAME_CHARS]


@contextmanager
def run_agent_tool_proxy(
    *,
    app: WarehouseAppConfig,
    run_id: str = "",
    allowed_tools: frozenset[str] = DEFAULT_AGENT_TOOL_ALLOWLIST,
    max_rows: int = DEFAULT_AGENT_TOOL_PROXY_MAX_ROWS,
    bind_host: str = "0.0.0.0",
    public_host: str = "host.docker.internal",
) -> Iterator[dict[str, str]]:
    container_token = secrets.token_urlsafe(32)
    # The run-scoped name travels all the way upstream, so a query in the app's
    # request log names the agent run that made it instead of a shared label.
    client_name = agent_client_name(app.client_name, run_id)
    upstream = WarehouseAppConfig(base_url=app.base_url, token=app.token, client_name=client_name)
    # One upstream call at a time, matching the old proxy: an agent that fans
    # out queries should not multiply load on the shared warehouse.
    upstream_lock = threading.Lock()
    public_base_url = ""

    class AgentToolProxyHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            # Signed object links carry their own HMAC credential and are
            # fetched by plain curl, so this path is bearer-free by design.
            if self.path.startswith(OBJECTS_PATH_PREFIX):
                self._stream_object()
                return
            if not self._authorized():
                return
            if self.path == API_TOOLS_PATH:
                self._list_tools()
                return
            self._write_error("not_found", "no such path: " + self.path, status=404)

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            # Read the body before any rejection: on a keep-alive connection an
            # undrained body becomes the head of the next request.
            body = self.rfile.read(int(self.headers.get("content-length", "0") or "0"))
            if not self._authorized():
                return
            if not self.path.startswith(API_TOOLS_PATH + "/"):
                self._write_error("not_found", "no such path: " + self.path, status=404)
                return
            name = self.path[len(API_TOOLS_PATH) + 1 :]
            if name not in allowed_tools:
                self._write_error(
                    "tool_not_found",
                    f"tool {name!r} is not available to agent runs; allowed tools: "
                    + ", ".join(sorted(allowed_tools)),
                    status=404,
                )
                return
            status, payload = self._forward_json("POST", API_TOOLS_PATH + "/" + name, body=body)
            if status == 200 and isinstance(payload, dict) and isinstance(payload.get("data"), dict):
                payload = {**payload, "data": self._shape_tool_output(name, payload["data"])}
            self._write_json(payload, status=status)

        # -- helpers -----------------------------------------------------

        def _authorized(self) -> bool:
            credential = str(self.headers.get("authorization") or "")
            prefix = "Bearer "
            presented = credential[len(prefix) :].strip() if credential.startswith(prefix) else ""
            _, _, token = presented.rpartition(":")
            if not token or not secrets.compare_digest(token, container_token):
                self._write_error("unauthorized", "invalid bearer credential", status=401)
                return False
            return True

        def _list_tools(self) -> None:
            status, payload = self._forward_json("GET", API_TOOLS_PATH)
            if status == 200 and isinstance(payload, dict) and isinstance(payload.get("data"), list):
                entries = [
                    entry
                    for entry in payload["data"]
                    if isinstance(entry, dict) and entry.get("name") in allowed_tools
                ]
                payload = {**payload, "data": entries}
            self._write_json(payload, status=status)

        def _shape_tool_output(self, name: str, data: dict[str, Any]) -> dict[str, Any]:
            if name == "sql":
                return cap_sql_rows(data, max_rows=max_rows)
            if name == "get_object":
                return rewrite_download_url(data, public_base_url=public_base_url)
            return data

        def _forward_json(self, method: str, path: str, *, body: bytes | None = None) -> tuple[int, Any]:
            headers = {
                "authorization": upstream.authorization,
                "accept": "application/json",
                "user-agent": UPSTREAM_USER_AGENT,
            }
            if body:
                headers["content-type"] = "application/json"
            req = request.Request(upstream.base_url + path, data=body or None, headers=headers, method=method)
            with upstream_lock:
                try:
                    with request.urlopen(req, timeout=UPSTREAM_TIMEOUT_SECONDS) as response:
                        return response.status, json.loads(response.read().decode("utf-8") or "{}")
                except error.HTTPError as exc:
                    raw = exc.read().decode("utf-8", errors="replace")
                    try:
                        return exc.code, json.loads(raw or "{}")
                    except json.JSONDecodeError:
                        return exc.code, {"error": {"code": "http_error", "message": raw or str(exc)}}
                except Exception as exc:  # noqa: BLE001 - surface transport failures to the agent
                    return 502, {"error": {"code": "upstream_unreachable", "message": str(exc)}}

        def _stream_object(self) -> None:
            # Copied through in chunks, not buffered: stored objects run to
            # 100 MB and this proxy lives inside the Dagster process.
            req = request.Request(
                upstream.base_url + self.path,
                headers={"user-agent": UPSTREAM_USER_AGENT},
                method="GET",
            )
            try:
                with request.urlopen(req, timeout=UPSTREAM_TIMEOUT_SECONDS) as response:
                    self.send_response(200)
                    self.send_header(
                        "content-type", response.headers.get("content-type", "application/octet-stream")
                    )
                    length = response.headers.get("content-length")
                    if length:
                        self.send_header("content-length", length)
                    else:
                        # Without an upstream length there is nothing to promise,
                        # so end the body by closing the connection.
                        self.send_header("connection", "close")
                        self.close_connection = True
                    disposition = response.headers.get("content-disposition", "")
                    if disposition:
                        self.send_header("content-disposition", disposition)
                    self.end_headers()
                    while chunk := response.read(OBJECT_STREAM_CHUNK_BYTES):
                        self.wfile.write(chunk)
            except error.HTTPError as exc:
                raw = exc.read()
                self.send_response(exc.code)
                self.send_header("content-type", "text/plain; charset=utf-8")
                self.send_header("content-length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            except Exception as exc:  # noqa: BLE001 - surface transport failures to the agent
                self._write_error("upstream_unreachable", str(exc), status=502)

        def _write_error(self, code: str, message: str, *, status: int) -> None:
            self._write_json({"error": {"code": code, "message": message}}, status=status)

        def _write_json(self, payload: Any, *, status: int = 200) -> None:
            encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, _format: str, *_args: Any) -> None:
            return

    server = ThreadingHTTPServer((bind_host, 0), AgentToolProxyHandler)
    public_base_url = f"http://{public_host}:{server.server_port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield {
            "PDW_API_URL": public_base_url,
            "PDW_SECRET_TOKEN": container_token,
            "PDW_CLIENT_NAME": client_name,
            # The CLI otherwise kicks a background GitHub self-update on every
            # invocation; the agent container is read-only and offline-ish.
            "PDW_NO_AUTO_UPDATE": "1",
        }
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def cap_sql_rows(data: Mapping[str, Any], *, max_rows: int) -> dict[str, Any]:
    """Bound one `pdw sql` result so a missing LIMIT cannot flood the agent.

    The app's own cap is 100k rows, which is fine for a human at a terminal and
    ruinous for an agent's context window. `total_rows` keeps reporting the real
    count so the agent can tell it is looking at a slice.
    """

    capped = dict(data)
    rows = capped.get("rows")
    if max_rows <= 0 or rows is None:
        return capped
    fmt = str(capped.get("format") or "csv").lower()
    if isinstance(rows, list):
        if len(rows) <= max_rows:
            return capped
        capped["rows"] = rows[:max_rows]
    elif isinstance(rows, str):
        lines = rows.splitlines()
        # CSV's first line is the header, so it does not count against the cap.
        keep = max_rows + 1 if fmt == "csv" else max_rows
        if len(lines) <= keep:
            return capped
        capped["rows"] = "\n".join(lines[:keep])
    else:
        return capped
    capped["truncated"] = True
    capped["proxy_row_limit"] = max_rows
    return capped


def rewrite_download_url(data: Mapping[str, Any], *, public_base_url: str) -> dict[str, Any]:
    """Point a signed object link back at this proxy.

    Upstream signs links against the app's public base URL. Agent containers are
    only guaranteed a route to the proxy, so the bytes come back the same way
    every other call does; the signature still travels untouched and is still
    verified by the app.
    """

    rewritten = dict(data)
    raw = str(rewritten.get("download_url") or "")
    if not raw or not public_base_url:
        return rewritten
    parsed = parse.urlsplit(raw)
    if not parsed.path.startswith(OBJECTS_PATH_PREFIX):
        return rewritten
    rewritten["download_url"] = public_base_url + parsed.path + (f"?{parsed.query}" if parsed.query else "")
    return rewritten
