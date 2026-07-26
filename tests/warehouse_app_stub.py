"""A stand-in for the Go app's `/api/tools` + `/objects` surface.

Shared by the agent tool-proxy unit tests and the live Docker tests, which
exercise the same proxy from opposite sides (in-process vs. from inside a
container), so the upstream contract only has to be modelled once.
"""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
from typing import Any

from personal_data_warehouse.agent_tool_proxy import DEFAULT_AGENT_TOOL_ALLOWLIST


class StubWarehouseApp:
    def __init__(
        self,
        *,
        tools: list[dict[str, Any]] | None = None,
        responses: dict[str, dict[str, Any]] | None = None,
        bind_host: str = "127.0.0.1",
        advertised_host: str = "",
    ) -> None:
        self.tools = (
            tools
            if tools is not None
            else [
                {"name": name, "title": name, "description": "", "input_schema": {}}
                for name in sorted(DEFAULT_AGENT_TOOL_ALLOWLIST | {"propose_mutation"})
            ]
        )
        self.responses = responses or {}
        self.calls: list[tuple[str, str, str, bytes]] = []
        self.object_bytes = b""

        app = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                app.calls.append(("GET", self.path, self.headers.get("authorization", ""), b""))
                if self.path == "/api/tools":
                    app.write_json(self, {"data": app.tools})
                    return
                if self.path.startswith("/objects/"):
                    self.send_response(200)
                    self.send_header("content-type", "application/octet-stream")
                    self.send_header("content-length", str(len(app.object_bytes)))
                    self.end_headers()
                    self.wfile.write(app.object_bytes)
                    return
                app.write_json(self, {"error": {"code": "not_found", "message": self.path}}, status=404)

            def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                body = self.rfile.read(int(self.headers.get("content-length", "0") or "0"))
                app.calls.append(("POST", self.path, self.headers.get("authorization", ""), body))
                name = self.path.removeprefix("/api/tools/")
                if name in app.responses:
                    app.write_json(self, {"data": app.responses[name]})
                    return
                app.write_json(self, {"error": {"code": "tool_not_found", "message": name}}, status=404)

            def log_message(self, *_args: Any) -> None:
                return

        self._server = ThreadingHTTPServer((bind_host, 0), Handler)
        self._advertised_host = advertised_host or bind_host
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @staticmethod
    def write_json(handler: BaseHTTPRequestHandler, payload: dict[str, Any], *, status: int = 200) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        handler.send_response(status)
        handler.send_header("content-type", "application/json")
        handler.send_header("content-length", str(len(encoded)))
        handler.end_headers()
        handler.wfile.write(encoded)

    @property
    def port(self) -> int:
        return self._server.server_port

    @property
    def base_url(self) -> str:
        return f"http://{self._advertised_host}:{self.port}"

    def tool_call_paths(self) -> list[str]:
        return [path for method, path, _auth, _body in self.calls if method == "POST"]

    def object_paths(self) -> list[str]:
        return [path for method, path, _auth, _body in self.calls if method == "GET" and path.startswith("/objects/")]

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)
