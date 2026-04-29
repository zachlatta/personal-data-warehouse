from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import secrets
import threading
from typing import Any

from personal_data_warehouse.clickhouse_readonly import ClickHouseReadOnlyService


@contextmanager
def run_agent_tool_proxy(
    *,
    query_service: ClickHouseReadOnlyService,
    bind_host: str = "0.0.0.0",
    public_host: str = "host.docker.internal",
) -> Iterator[dict[str, str]]:
    token = secrets.token_urlsafe(32)

    class AgentToolProxyHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            if self.headers.get("authorization") != f"Bearer {token}":
                self._write_json({"error": "unauthorized"}, status=401)
                return
            try:
                body = self.rfile.read(int(self.headers.get("content-length", "0") or "0"))
                payload = json.loads(body.decode("utf-8") or "{}")
            except Exception as exc:
                self._write_json({"error": f"invalid JSON request: {exc}"}, status=400)
                return
            if self.path == "/query":
                sql = str(payload.get("sql") or "")
                self._write_json(query_service.execute_one(sql).as_tool_payload())
                return
            if self.path == "/schema":
                self._write_json(query_service.schema_overview().as_tool_payload())
                return
            self._write_json({"error": "not found"}, status=404)

        def log_message(self, _format: str, *_args: Any) -> None:
            return

        def _write_json(self, payload: dict[str, Any], *, status: int = 200) -> None:
            encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    server = ThreadingHTTPServer((bind_host, 0), AgentToolProxyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield {
            "PDW_AGENT_TOOL_PROXY_URL": f"http://{public_host}:{server.server_port}",
            "PDW_AGENT_TOOL_PROXY_TOKEN": token,
        }
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
